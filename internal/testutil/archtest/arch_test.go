package archtest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"testing"
)

// PackageInfo captures package import path and dependency metadata.
type PackageInfo struct {
	ImportPath string   `json:"ImportPath"`
	Standard   bool     `json:"Standard"`
	Imports    []string `json:"Imports"`
}

// CheckArchitecture analyzes packages according to TECH_SPEC §2.3 dependency rules.
// Returns a list of human-readable violation descriptions.
func CheckArchitecture(pkgs []PackageInfo) []string {
	var violations []string

	for _, p := range pkgs {
		pkgPath := p.ImportPath

		// Rule 1: domain -> stdlib only
		if pkgPath == "arus/internal/domain" || strings.HasPrefix(pkgPath, "arus/internal/domain/") {
			for _, imp := range p.Imports {
				if !isStandardLib(imp) {
					violations = append(violations, fmt.Sprintf("domain violation: %s imports non-stdlib %s", pkgPath, imp))
				}
			}
		}

		// Rule 2: app/* and engine -> domain + app/ports only (prohibited from importing adapter/*)
		if strings.HasPrefix(pkgPath, "arus/internal/app/") || pkgPath == "arus/internal/engine" || strings.HasPrefix(pkgPath, "arus/internal/engine/") {
			for _, imp := range p.Imports {
				if strings.HasPrefix(imp, "arus/internal/adapter/") {
					violations = append(violations, fmt.Sprintf("layer violation: %s must not import adapter %s", pkgPath, imp))
				}
			}
		}

		// Rule 3: adapter/* -> cannot import other adapters (except adapter/platform)
		if strings.HasPrefix(pkgPath, "arus/internal/adapter/") {
			srcAdapter := extractAdapterName(pkgPath)
			for _, imp := range p.Imports {
				if strings.HasPrefix(imp, "arus/internal/adapter/") {
					targetAdapter := extractAdapterName(imp)
					if srcAdapter != targetAdapter && targetAdapter != "platform" {
						violations = append(violations, fmt.Sprintf("cross-adapter violation: %s must not import adapter %s", pkgPath, imp))
					}
				}
			}
		}
	}

	return violations
}

func isStandardLib(path string) bool {
	if strings.HasPrefix(path, "arus/") {
		return false
	}
	firstPart := strings.Split(path, "/")[0]
	return !strings.Contains(firstPart, ".")
}

func extractAdapterName(pkgPath string) string {
	rest := strings.TrimPrefix(pkgPath, "arus/internal/adapter/")
	parts := strings.Split(rest, "/")
	if len(parts) > 0 {
		return parts[0]
	}
	return rest
}

// TestArchitecture_LiveRepo validates that the actual repository code strictly adheres to §2.3 dependency rules.
func TestArchitecture_LiveRepo(t *testing.T) {
	cmd := exec.Command("go", "list", "-json", "./...")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("failed to run 'go list -json ./...': %v", err)
	}

	decoder := json.NewDecoder(bytes.NewReader(out))
	var pkgs []PackageInfo

	for decoder.More() {
		var p PackageInfo
		if err := decoder.Decode(&p); err != nil {
			t.Fatalf("failed to decode package info: %v", err)
		}
		pkgs = append(pkgs, p)
	}

	violations := CheckArchitecture(pkgs)
	if len(violations) > 0 {
		for _, v := range violations {
			t.Errorf("Architecture rule violation: %s", v)
		}
	}
}

// TestArchitecture_SyntheticViolations asserts that rule violations are properly caught and reported.
func TestArchitecture_SyntheticViolations(t *testing.T) {
	tests := []struct {
		name          string
		pkgs          []PackageInfo
		wantViolation bool
		expectedSub   string
	}{
		{
			name: "domain imports external dependency",
			pkgs: []PackageInfo{
				{
					ImportPath: "arus/internal/domain",
					Imports:    []string{"fmt", "github.com/google/uuid"},
				},
			},
			wantViolation: true,
			expectedSub:   "domain violation: arus/internal/domain imports non-stdlib github.com/google/uuid",
		},
		{
			name: "domain imports internal app ports",
			pkgs: []PackageInfo{
				{
					ImportPath: "arus/internal/domain",
					Imports:    []string{"arus/internal/app/ports"},
				},
			},
			wantViolation: true,
			expectedSub:   "domain violation: arus/internal/domain imports non-stdlib arus/internal/app/ports",
		},
		{
			name: "app/manager imports adapter/sqlite",
			pkgs: []PackageInfo{
				{
					ImportPath: "arus/internal/app/manager",
					Imports:    []string{"arus/internal/domain", "arus/internal/adapter/sqlite"},
				},
			},
			wantViolation: true,
			expectedSub:   "layer violation: arus/internal/app/manager must not import adapter arus/internal/adapter/sqlite",
		},
		{
			name: "engine imports adapter/httpfetch",
			pkgs: []PackageInfo{
				{
					ImportPath: "arus/internal/engine",
					Imports:    []string{"arus/internal/app/ports", "arus/internal/adapter/httpfetch"},
				},
			},
			wantViolation: true,
			expectedSub:   "layer violation: arus/internal/engine must not import adapter arus/internal/adapter/httpfetch",
		},
		{
			name: "adapter/sqlite imports adapter/httpfetch (illegal cross-adapter)",
			pkgs: []PackageInfo{
				{
					ImportPath: "arus/internal/adapter/sqlite",
					Imports:    []string{"arus/internal/adapter/httpfetch"},
				},
			},
			wantViolation: true,
			expectedSub:   "cross-adapter violation: arus/internal/adapter/sqlite must not import adapter arus/internal/adapter/httpfetch",
		},
		{
			name: "adapter/sqlite imports adapter/platform (allowed exception §2.3)",
			pkgs: []PackageInfo{
				{
					ImportPath: "arus/internal/adapter/sqlite",
					Imports:    []string{"arus/internal/adapter/platform"},
				},
			},
			wantViolation: false,
		},
		{
			name: "cmd/arusd imports adapters and engine (allowed composition root)",
			pkgs: []PackageInfo{
				{
					ImportPath: "arus/cmd/arusd",
					Imports: []string{
						"arus/internal/domain",
						"arus/internal/engine",
						"arus/internal/adapter/sqlite",
						"arus/internal/adapter/fsx",
					},
				},
			},
			wantViolation: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			violations := CheckArchitecture(tc.pkgs)
			if tc.wantViolation {
				if len(violations) == 0 {
					t.Fatalf("expected architecture violation, but got none")
				}
				found := false
				for _, v := range violations {
					if strings.Contains(v, tc.expectedSub) {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("expected violation substring %q, got %v", tc.expectedSub, violations)
				}
			} else if len(violations) > 0 {
				t.Errorf("expected no violations, got: %v", violations)
			}
		})
	}
}
