package version

// Version is the application version, overridden at build time via -ldflags.
var Version = "dev"

// Commit is the git commit SHA, overridden at build time via -ldflags.
var Commit = "none"

// Date is the build timestamp, overridden at build time via -ldflags.
var Date = "unknown"
