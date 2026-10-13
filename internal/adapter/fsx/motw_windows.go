//go:build windows

package fsx

import (
	"fmt"
	"os"
)

// WriteZoneIdentifier writes the NTFS Alternate Data Stream :Zone.Identifier (ZoneId=3)
// to mark files as downloaded from the internet for SmartScreen and Office Protected View.
func WriteZoneIdentifier(path, referrer, hostURL string) error {
	adsPath := toExtendedPath(path) + ":Zone.Identifier"
	content := fmt.Sprintf("[ZoneTransfer]\r\nZoneId=3\r\nReferrerUrl=%s\r\nHostUrl=%s\r\n", referrer, hostURL)
	return os.WriteFile(adsPath, []byte(content), 0o600)
}
