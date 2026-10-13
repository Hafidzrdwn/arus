//go:build !windows

package fsx

// WriteZoneIdentifier is a no-op on non-Windows platforms.
func WriteZoneIdentifier(_, _, _ string) error {
	return nil
}
