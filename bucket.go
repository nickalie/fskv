package fskv

import (
	"errors"
	"github.com/spf13/afero"
	"os"
	"path/filepath"
	"strings"
)

type Bucket struct {
	dir  string
	pool *pool
}

func (b *Bucket) GetBucket(name string) (*Bucket, error) {
	dir := filepath.Join(b.dir, name)
	fs := b.pool.Get()
	defer b.pool.Put(fs)
	err := fs.MkdirAll(dir, 0755)

	if err != nil {
		return nil, err
	} else {
		return &Bucket{dir: dir, pool: b.pool}, nil
	}
}

func (b *Bucket) Set(key string, value []byte) error {
	fileName := filepath.Join(b.dir, key)
	fs := b.pool.Get()
	defer b.pool.Put(fs)
	l, err := getLock(fs, fileName)

	if err != nil {
		return err
	}

	defer l.unlock(fs)

	return afero.WriteFile(fs, fileName, value, 0755)
}

func (b *Bucket) Get(key string) ([]byte, error) {
	fileName := filepath.Join(b.dir, key)
	fs := b.pool.Get()
	defer b.pool.Put(fs)
	return afero.ReadFile(fs, fileName)
}

func (b *Bucket) Scan(prefix string, f func(key string, value []byte) bool) {
	fs := b.pool.Get()
	defer b.pool.Put(fs)

	afero.Walk(fs, b.dir, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() || filepath.Ext(info.Name()) == ".lock" || !strings.HasPrefix(info.Name(), prefix) {
			return nil
		}

		value, err := afero.ReadFile(fs, path)

		if err != nil {
			return err
		}

		result := f(info.Name(), value)

		if !result {
			return errors.New("Stop")
		}

		return nil
	})
}

func (b *Bucket) Remove(keys ...string) error {
	fs := b.pool.Get()
	defer b.pool.Put(fs)
	if len(keys) == 0 {
		return fs.RemoveAll(b.dir)
	}

	for _, v := range keys {
		v = filepath.Join(b.dir, v)

		l, err := getLock(fs, v)

		if err != nil {
			return err
		}

		err = fs.RemoveAll(v)
		l.unlock(fs)

		if err != nil {
			return err
		}
	}

	files, err := afero.ReadDir(fs, b.dir)

	if err != nil {
		return err
	}

	if len(files) == 0 && err == nil {
		return fs.RemoveAll(b.dir)
	}

	return nil
}
