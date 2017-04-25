package fskv

import (
	"bytes"
	"errors"
	"github.com/spf13/afero"
	"math/rand"
	"strconv"
)

// ErrLocked returned in case is key locked for modifications
var ErrLocked = errors.New("Locked")

type lock struct {
	path string
	id   []byte
}

func getLock(fs afero.Fs, path string) (*lock, error) {
	lockFile := path + ".lock"

	exists, err := afero.Exists(fs, lockFile)

	if err == nil && exists {
		return nil, ErrLocked
	}

	id := []byte(strconv.FormatUint(rand.Uint64(), 10))
	err = afero.WriteFile(fs, lockFile, id, 0777)

	if err != nil {
		return nil, err
	}

	return &lock{path: lockFile, id: id}, nil
}

func (l *lock) unlock(fs afero.Fs) error {
	id, err := afero.ReadFile(fs, l.path)

	if err != nil {
		return err
	}

	if !bytes.Equal(l.id, id) {
		return ErrLocked
	}

	return fs.RemoveAll(l.path)
}
