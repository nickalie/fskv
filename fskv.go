// Package fskv provides simple file system based key-value storage.
package fskv

import (
	"github.com/spf13/afero"
	"sync"
)

type pool struct {
	*sync.Pool
}

func (p *pool) Get() afero.Fs {
	return p.Pool.Get().(afero.Fs)
}

// DB represents a collection of buckets and key-value pairs that persist on disk.
type DB struct {
	root *Bucket
}

// Open creates and opens a database at the given path. If the directory does not exist then it will be created automatically.
func Open(dir string) (*DB, error) {
	return OpenWithFactory(func() interface{} {
		return afero.NewBasePathFs(afero.NewOsFs(), dir)
	})
}

// OpenWithFactory creates and opens a database with the given function. Function should return afero.Fs
func OpenWithFactory(fn func() interface{}) (*DB, error) {
	p := &pool{
		&sync.Pool{
			New: fn,
		},
	}

	fs := p.Get()
	defer p.Put(fs)
	fs.MkdirAll("", 0755)
	return &DB{&Bucket{dir: "", pool: p}}, nil
}

// GetBucket creates a new bucket if it doesn't already exist and returns a reference to it. Returns an error if the bucket name is invalid.
func (b *DB) GetBucket(name string) (*Bucket, error) {
	return b.root.GetBucket(name)
}

// Set sets the value for a key. If the key exist then its previous value will be overwritten.
func (b *DB) Set(key string, value []byte) error {
	return b.root.Set(key, value)
}

// Get retrieves the value for a key. Returns an error value if the key does not exist.
func (b *DB) Get(key string) ([]byte, error) {
	return b.root.Get(key)
}

// Scan executes a function for each key/value pair in a bucket if key has prefix. If the provided function returns false then the iteration is stopped.
func (b *DB) Scan(prefix string, f func(key string, value []byte) bool) {
	b.root.Scan(prefix, f)
}

// Remove removes a key. If the key does not exist then nothing is done. If no keys provided whole content of the storage wil be removed.
func (b *DB) Remove(keys ...string) error {
	return b.root.Remove(keys...)
}
