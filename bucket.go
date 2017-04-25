package fskv

import (
	"errors"
	"github.com/spf13/afero"
	"os"
	"path/filepath"
	"strings"
)

// Bucket represents a collection of key/value pairs inside the storage.
type Bucket struct {
	dir  string
	pool *pool
}

// GetBucket creates a new bucket if it doesn't already exist and returns a reference to it. Returns an error if the bucket name is invalid.
func (b *Bucket) GetBucket(name string) (*Bucket, error) {
	dir := filepath.Join(b.dir, name)
	fs := b.pool.Get()
	defer b.pool.Put(fs)
	err := fs.MkdirAll(dir, 0755)

	if err != nil {
		return nil, err
	}

	return &Bucket{dir: dir, pool: b.pool}, nil
}

// Set sets the value for a key in the bucket. If the key exist then its previous value will be overwritten.
func (b *Bucket) Set(key string, value []byte) error {
	fileName := filepath.Join(b.dir, key)
	fs := b.pool.Get()
	defer b.pool.Put(fs)
	l, err := getLock(fs, fileName)

	if err != nil {
		return err
	}

	err = afero.WriteFile(fs, fileName, value, 0755)

	if err != nil {
		l.unlock(fs)
		return err
	}

	return l.unlock(fs)
}

// Get retrieves the value for a key in the bucket. Returns an error value if the key does not exist.
func (b *Bucket) Get(key string) ([]byte, error) {
	fileName := filepath.Join(b.dir, key)
	fs := b.pool.Get()
	defer b.pool.Put(fs)
	return afero.ReadFile(fs, fileName)
}

// Scan executes a function for each key/value pair in a bucket if key has prefix. If the provided function returns false then the iteration is stopped.
func (b *Bucket) Scan(prefix string, fn func(key string, value []byte) bool) {
	fs := b.pool.Get()
	defer b.pool.Put(fs)

	afero.Walk(fs, b.dir, func(path string, info os.FileInfo, err error) error {

		if info == nil {
			return nil
		}

		if info.IsDir() || filepath.Ext(info.Name()) == ".lock" || !strings.HasPrefix(info.Name(), prefix) {
			return nil
		}

		value, err := afero.ReadFile(fs, path)

		if err != nil {
			return err
		}

		result := fn(info.Name(), value)

		if !result {
			return errors.New("Stop")
		}

		return nil
	})
}

// Remove removes a key from the bucket. If the key does not exist then nothing is done. If no keys provided whole bucket will be removed.
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

		l.unlock(fs)
		err = fs.RemoveAll(v)

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
