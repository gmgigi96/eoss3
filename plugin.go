package main

import (
	"os"

	"github.com/gmgigi96/eoss3/eoss3"
	"github.com/gmgigi96/eoss3/meta"
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
	defer f.Close()

	var m map[string]any
	if err := yaml.NewDecoder(f).Decode(&m); err != nil {
		return nil, err
	}

	var cfg eoss3.Config
	if err := mapstructure.Decode(m, &cfg); err != nil {
		return nil, err
	}

	regCfg, ok := m["buckets"].(map[string]any)
	if !ok {
		regCfg = make(map[string]any)
	}
	buckets, err := meta.New(regCfg)
	if err != nil {
		return nil, err
	}

	be, err := eoss3.New(&cfg, buckets)
	if err != nil {
		return nil, err
	}
	return be, nil
}
