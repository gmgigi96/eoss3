package main

import (
	"github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/plugins"
)

var Backend plugins.BackendPlugin = plugin{}

type plugin struct{}

func (plugin) New(m map[string]any) (backend.Backend, error) {
	return &EosBackend{}, nil
}
