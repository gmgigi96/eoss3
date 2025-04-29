package eos

import "fmt"

type ErrNoSuchResource struct {
	Path string
}

func (e ErrNoSuchResource) Error() string {
	return fmt.Sprintf("no such resource: %s", e.Path)
}
