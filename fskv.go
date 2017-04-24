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

type FSKV struct {
	root *Bucket
}

func NewFSKV(dir string) (*FSKV, error) {
	return NewFSKVWithFactory(func() interface{} {
		return afero.NewBasePathFs(afero.NewOsFs(), dir)
	})
}

func NewFSKVWithFactory(f func() interface{}) (*FSKV, error) {
	p := &pool{
		&sync.Pool{
			New: f,
		},
	}

	fs := p.Get()
	defer p.Put(fs)
	fs.MkdirAll("", 0755)
	return &FSKV{&Bucket{dir: "", pool: p}}, nil
}

func (b *FSKV) GetBucket(name string) (*Bucket, error) {
	return b.root.GetBucket(name)
}

func (b *FSKV) Set(key string, value []byte) error {
	return b.root.Set(key, value)
}

func (b *FSKV) Get(key string) ([]byte, error) {
	return b.root.Get(key)
}

func (b *FSKV) Scan(prefix string, f func(key string, value []byte) bool) {
	b.root.Scan(prefix, f)
}

func (b *FSKV) Remove(keys ...string) error {
	return b.root.Remove(keys...)
}
