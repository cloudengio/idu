// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package config

import (
	"fmt"

	"cloudeng.io/file/diskusage"
)

type layoutSpec struct {
	Type      string `yaml:"type" cmd:"type of this layout"`
	Prefix    string `yaml:"prefix" cmd:"prefix that this layout applies to"`
	Separator string `yaml:"separator" cmd:"filename separator to use, defaults tp /"`
}

type layout struct {
	Spec     layoutSpec `yaml:",inline"`
	instance diskusage.Calculator
}

type layoutConfig struct {
	config  interface{}
	factory func(cfg interface{}) (diskusage.Calculator, error)
}

var supportedLayouts = map[string]layoutConfig{
	"block":    {&simple{}, newSimpleLayout},
	"identity": {&identity{}, newIdentity},
	"raid0":    {&raid0{}, newRaid0},
}

func (l *layout) UnmarshalYAML(unmarshal func(interface{}) error) error {
	if err := unmarshal(&l.Spec); err != nil {
		return err
	}
	cfg, ok := supportedLayouts[l.Spec.Type]
	if !ok {
		return fmt.Errorf("unsupported layout: %v %v", l.Spec.Type, l.Spec.Prefix)
	}
	if err := unmarshal(cfg.config); err != nil {
		return err
	}
	instance, err := cfg.factory(cfg.config)
	if err != nil {
		return fmt.Errorf("failed to configure %v for prefix %v: %v", l.Spec.Type, l.Spec.Prefix, err)
	}
	l.instance = instance
	return err
}

type simple struct {
	BlockSize int64 `yaml:"block_size" cmd:"block size used by this filesystem"`
}

type identity struct{}

type raid0 struct {
	StripeSize int64 `yaml:"stripe_size" cmd:"the size of the raid0 stripes"`
	NumStripes int   `yaml:"num_stripes" cmd:"the number of stripes used"`
}

func newSimpleLayout(cfg interface{}) (diskusage.Calculator, error) {
	c := cfg.(*simple)
	if s := c.BlockSize; s == 0 {
		return nil, fmt.Errorf("invalid block size: %v", s)
	}
	return diskusage.NewSimple(c.BlockSize), nil
}

func newIdentity(cfg interface{}) (diskusage.Calculator, error) {
	return diskusage.NewIdentity(), nil
}

func newRaid0(cfg interface{}) (diskusage.Calculator, error) {
	c := cfg.(*raid0)
	if s := c.StripeSize; s == 0 {
		return nil, fmt.Errorf("invalid stripe size: %v", s)
	}
	if s := c.NumStripes; s == 0 {
		return nil, fmt.Errorf("invalid number of stripes: %v", s)
	}
	return diskusage.NewRAID0(c.StripeSize, c.NumStripes), nil
}
