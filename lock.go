package fskv

import (
	"errors"
	"github.com/spf13/afero"
)

type lock struct {
	file string
}

func getLock(fs afero.Fs, name string) (*lock, error) {
	lockFile := name + ".lock"
	r, err := afero.Exists(fs, lockFile)

	if err != nil {
		return nil, err
	}

	if r {
		return nil, errors.New("Locked")
	}

	f, err := fs.Create(lockFile)

	if err != nil {
		return nil, err
	}

	defer f.Close()
	return &lock{file: lockFile}, nil
}

func (l *lock) unlock(fs afero.Fs) error {
	return fs.RemoveAll(l.file)
}
