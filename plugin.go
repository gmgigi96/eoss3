package main

import (
	"os"

	"github.com/gmgigi96/eoss3/registry"
	"github.com/mitchellh/mapstructure"
	"github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/plugins"
	yaml "sigs.k8s.io/yaml/goyaml.v3"
)

var Backend plugins.BackendPlugin = plugin{}

type plugin struct{}

func (plugin) New(config string) (backend.Backend, error) {
	f, err := os.Open(config)
	if err != nil {
		return nil, err
	}

	var m map[string]any
	if err := yaml.NewDecoder(f).Decode(&m); err != nil {
		return nil, err
	}

	var cfg Config
	if err := mapstructure.Decode(m, &cfg); err != nil {
		return nil, err
	}

	regCfg, ok := m["registry"].(map[string]any)
	if !ok {
		regCfg = make(map[string]any)
	}
	registry, err := registry.New(regCfg)
	if err != nil {
		return nil, err
	}

	be, err := New(&cfg, registry)
	if err != nil {
		return nil, err
	}
	return be, nil
}
