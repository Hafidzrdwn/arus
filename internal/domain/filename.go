package domain

import (
	"path/filepath"
	"strings"
	"unicode/utf8"
)

var windowsReservedNames = map[string]struct{}{
	"CON":    {},
	"PRN":    {},
	"AUX":    {},
	"NUL":    {},
	"COM1":   {},
	"COM2":   {},
	"COM3":   {},
	"COM4":   {},
	"COM5":   {},
	"COM6":   {},
	"COM7":   {},
	"COM8":   {},
	"COM9":   {},
	"LPT1":   {},
	"LPT2":   {},
	"LPT3":   {},
	"LPT4":   {},
	"LPT5":   {},
	"LPT6":   {},
	"LPT7":   {},
	"LPT8":   {},
	"LPT9":   {},
	"CLOCK$": {},
}

// isBidiOverride checks if a rune is a Unicode BiDi override or isolate character.
func isBidiOverride(r rune) bool {
	// U+202A - U+202E: LRE, RLE, PDF, LRO, RLO
	if r >= 0x202A && r <= 0x202E {
		return true
	}
	// U+2066 - U+2069: LRI, RLI, FSI, PDI
	if r >= 0x2066 && r <= 0x2069 {
		return true
	}
	return false
}

// isIllegalChar checks if a rune is a filesystem illegal or control character.
func isIllegalChar(r rune) bool {
	if r < 32 || r == 127 {
		return true
	}
	switch r {
	case '<', '>', ':', '"', '/', '\\', '|', '?', '*':
		return true
	}
	return false
}

// SanitizeFileName cleans and sanitizes a raw file name according to TECH_SPEC §6.14:
// - Strips path traversal sequences ("..")
// - Removes illegal filesystem characters (<>:"/\|?*) and control characters
// - Removes Unicode BiDi override characters (U+202A-U+202E, U+2066-U+2069)
// - Trims trailing dots and spaces
// - Prefixes Windows reserved device names (CON, PRN, AUX, NUL, COM1-9, LPT1-9 ± extension) with '_'
// - Enforces a maximum length of 200 runes (preserving extension when possible)
// - Falls back to "download" if the resulting string is empty
// - Preserves valid Unicode non-ASCII characters
func SanitizeFileName(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "download"
	}

	cleaned := raw
	for strings.Contains(cleaned, "..") {
		cleaned = strings.ReplaceAll(cleaned, "..", "")
	}

	var sb strings.Builder
	for _, r := range cleaned {
		if isIllegalChar(r) || isBidiOverride(r) {
			continue
		}
		sb.WriteRune(r)
	}
	cleaned = sb.String()

	cleaned = strings.TrimRight(cleaned, ". ")
	if cleaned == "" {
		return "download"
	}

	stem := cleaned
	if idx := strings.IndexByte(cleaned, '.'); idx != -1 {
		stem = cleaned[:idx]
	}
	stemUpper := strings.ToUpper(strings.TrimSpace(stem))
	if _, isReserved := windowsReservedNames[stemUpper]; isReserved {
		cleaned = "_" + cleaned
	}

	runeCount := utf8.RuneCountInString(cleaned)
	if runeCount > 200 {
		runes := []rune(cleaned)
		ext := filepath.Ext(cleaned)
		extRunes := []rune(ext)

		if len(extRunes) > 0 && len(extRunes) < 50 && 200-len(extRunes) > 0 {
			stemLimit := 200 - len(extRunes)
			stem := strings.TrimRight(string(runes[:stemLimit]), ". ")
			cleaned = stem + ext
		} else {
			cleaned = string(runes[:200])
		}
		cleaned = strings.TrimRight(cleaned, ". ")
		if cleaned == "" {
			return "download"
		}
	}

	return cleaned
}
