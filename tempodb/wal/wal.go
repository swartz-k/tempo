package wal

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/google/uuid"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/local"
)

const (
	completedDir = "completed"
	blocksDir    = "blocks"
)

type WAL struct {
	c *Config
	l *local.Backend
}

type Config struct {
	Filepath          string `yaml:"path"`
	CompletedFilepath string
	BlocksFilepath    string
	Encoding          backend.Encoding `yaml:"encoding"`
}

func New(c *Config) (*WAL, error) {
	if c.Filepath == "" {
		return nil, fmt.Errorf("please provide a path for the WAL")
	}

	// make folder
	err := os.MkdirAll(c.Filepath, os.ModePerm)
	if err != nil {
		return nil, err
	}

	// The /completed/ folder is now obsolete and no new data is written,
	// but it needs to be cleared out one last time for any files left
	// from a previous version.
	if c.CompletedFilepath == "" {
		completedFilepath := filepath.Join(c.Filepath, completedDir)
		err = os.RemoveAll(completedFilepath)
		if err != nil {
			return nil, err
		}

		c.CompletedFilepath = completedFilepath
	}

	// Setup local backend in /blocks/
	p := filepath.Join(c.Filepath, blocksDir)
	err = os.MkdirAll(p, os.ModePerm)
	if err != nil {
		return nil, err
	}
	c.BlocksFilepath = p

	l, err := local.NewBackend(&local.Config{
		Path: p,
	})
	if err != nil {
		return nil, err
	}

	return &WAL{
		c: c,
		l: l,
	}, nil
}

func (w *WAL) AllBlocks() ([]*ReplayBlock, error) {
	files, err := ioutil.ReadDir(w.c.Filepath)
	if err != nil {
		return nil, err
	}

	blocks := make([]*ReplayBlock, 0, len(files))
	for _, f := range files {
		if f.IsDir() {
			continue
		}

		r, err := NewReplayBlock(f.Name(), w.c.Filepath)
		if err != nil {
			return nil, err
		}

		blocks = append(blocks, r)
	}

	return blocks, nil
}

func (w *WAL) NewBlock(id uuid.UUID, tenantID string) (*AppendBlock, error) {
	return newAppendBlock(id, tenantID, w.c.Filepath, w.c.Encoding)
}

func (w *WAL) LocalBackend() *local.Backend {
	return w.l
}
