package domain

// AllocMode represents disk pre-allocation strategy.
type AllocMode string

// Disk allocation modes specified in TECH_SPEC §6.5.
const (
	AllocModeSparse AllocMode = "sparse"
	AllocModeFull   AllocMode = "full"
	AllocModeNone   AllocMode = "none"
)

// FSInfo holds filesystem characteristics and capacity limits.
type FSInfo struct {
	FSName      string `json:"fs_name"`
	MaxFileSize int64  `json:"max_file_size"`
	FreeBytes   int64  `json:"free_bytes"`
}
