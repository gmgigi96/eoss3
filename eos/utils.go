package eos

import "strings"

// IsVersionFolder returns true if the resource is a version folder.
func IsVersionFolder(path string) bool {
	return strings.Contains(path, ".sys.v#.")
}

// IsAtomicFile returns true if the resource is an atomic file.
func IsAtomicFile(path string) bool {
	return strings.Contains(path, ".sys.a#")
}
