//go:build windows

package fsx_test

import (
	"arus/internal/adapter/fsx"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMOTW_Windows_WriteAndReadZoneIdentifier(t *testing.T) {
	tempDir := t.TempDir()
	targetFile := filepath.Join(tempDir, "sample.exe")
	if err := os.WriteFile(targetFile, []byte("fake binary"), 0o600); err != nil {
		t.Fatalf("WriteFile error: %v", err)
	}

	referrer := "https://example.com/page"
	hostURL := "https://cdn.example.com/sample.exe"
	if err := fsx.WriteZoneIdentifier(targetFile, referrer, hostURL); err != nil {
		t.Fatalf("WriteZoneIdentifier error: %v", err)
	}

	adsPath := targetFile + ":Zone.Identifier"
	//nolint:gosec // reading test file
	data, err := os.ReadFile(adsPath)
	if err != nil {
		t.Fatalf("ReadFile on ADS :Zone.Identifier error: %v", err)
	}

	content := string(data)
	if !strings.Contains(content, "ZoneId=3") {
		t.Errorf("expected ZoneId=3 in ADS, got: %q", content)
	}
	if !strings.Contains(content, referrer) {
		t.Errorf("expected ReferrerUrl=%s in ADS, got: %q", referrer, content)
	}
	if !strings.Contains(content, hostURL) {
		t.Errorf("expected HostUrl=%s in ADS, got: %q", hostURL, content)
	}
}
