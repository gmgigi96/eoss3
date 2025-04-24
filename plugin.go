package main

import (
	"fmt"

	"github.com/mitchellh/mapstructure"
	"github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/plugins"
)

var Backend plugins.BackendPlugin = plugin{}

type plugin struct{}

func (plugin) New(m map[string]any) (backend.Backend, error) {
	fmt.Println(m)
	var cfg Config
	if err := mapstructure.Decode(m, &cfg); err != nil {
		return nil, err
	}
	be, err := New(&cfg)
	if err != nil {
		return nil, err
	}
	return be, nil
}
