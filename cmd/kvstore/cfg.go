package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"

	"github.com/galdor/go-raft/pkg/raft"
)

type Cfg struct {
	Servers raft.ServerSet `json:"servers"`
}

func DefaultCfg() *Cfg {
	cfg := &Cfg{
		Servers: make(raft.ServerSet),
	}

	return cfg
}

func (cfg *Cfg) LoadFile(filePath string) error {
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("cannot read %s: %w", filePath, err)
	}

	if err := json.Unmarshal(data, cfg); err != nil {
		return fmt.Errorf("cannot decode json data: %w", err)
	}

	return nil
}
