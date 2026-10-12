package domain_test

import (
	"arus/internal/domain"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestSanitizeFileName(t *testing.T) {
	// Generate a 500-rune string
	longRuneString := strings.Repeat("a", 500) + ".zip"

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Windows reserved CON",
			input:    "CON",
			expected: "_CON",
		},
		{
			name:     "Windows reserved nul with extension",
			input:    "nul.txt",
			expected: "_nul.txt",
		},
		{
			name:     "Windows reserved aux lowercase",
			input:    "aux",
			expected: "_aux",
		},
		{
			name:     "Windows reserved com1 multi-extension",
			input:    "com1.tar.gz",
			expected: "_com1.tar.gz",
		},
		{
			name:     "Bidi override RLO character",
			input:    "a\u202Eb.exe",
			expected: "ab.exe",
		},
		{
			name:     "Bidi isolate characters",
			input:    "foo\u2066bar\u2069.pdf",
			expected: "foobar.pdf",
		},
		{
			name:     "Path traversal double dots",
			input:    `..\..\x`,
			expected: "x",
		},
		{
			name:     "Trailing dot trimmed",
			input:    "file.",
			expected: "file",
		},
		{
			name:     "Trailing spaces trimmed",
			input:    "document   ",
			expected: "document",
		},
		{
			name:     "Trailing dots and spaces mixed",
			input:    "archive. . . ",
			expected: "archive",
		},
		{
			name:     "Illegal filesystem characters",
			input:    `bad<file>name:"with"/pipe|and?asterisk*.bin`,
			expected: "badfilenamewithpipeandasterisk.bin",
		},
		{
			name:     "Control characters removed",
			input:    "line\nbreak\r\x00and\x1Ftab\t.txt",
			expected: "linebreakandtab.txt",
		},
		{
			name:     "Valid Unicode non-ASCII preserved",
			input:    "Unduhan_Dokumen_日本語_résumé_файл.pdf",
			expected: "Unduhan_Dokumen_日本語_résumé_файл.pdf",
		},
		{
			name:     "Empty string falls back to download",
			input:    "",
			expected: "download",
		},
		{
			name:     "Only dots and spaces falls back to download",
			input:    "   ...   ",
			expected: "download",
		},
		{
			name:     "Only traversal characters falls back to download",
			input:    "../../",
			expected: "download",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := domain.SanitizeFileName(tc.input)
			if got != tc.expected {
				t.Errorf("SanitizeFileName(%q)\n got:  %q\n want: %q", tc.input, got, tc.expected)
			}
		})
	}

	// Test 500 rune length limitation (must be <= 200 runes and preserve .zip)
	t.Run("500 runes truncated to 200 runes preserving extension", func(t *testing.T) {
		got := domain.SanitizeFileName(longRuneString)
		count := utf8.RuneCountInString(got)
		if count > 200 {
			t.Errorf("expected rune count <= 200, got %d", count)
		}
		if !strings.HasSuffix(got, ".zip") {
			t.Errorf("expected extension .zip to be preserved, got: %s", got)
		}
	})
}
