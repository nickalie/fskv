package fskv

import (
	"github.com/stretchr/testify/assert"
	"math/rand"
	"path/filepath"
	"testing"
	"time"
)

func TestSet(t *testing.T) {
	db, err := NewFSKV("data")
	assert.Nil(t, err)
	err = db.Set(randString(10), []byte(randString(20)))
	assert.Nil(t, err)

}

func TestGetNotExists(t *testing.T) {
	db, err := NewFSKV("data")
	assert.Nil(t, err)
	data, err := db.Get(randString(10))
	assert.NotNil(t, err)
	assert.Nil(t, data)
}

func TestGet(t *testing.T) {
	db, err := NewFSKV("data")
	assert.Nil(t, err)
	key := randString(10)
	value := randString(20)
	err = db.Set(key, []byte(value))
	assert.Nil(t, err)
	data, err := db.Get(key)
	assert.Nil(t, err)
	assert.Equal(t, string(data), value)
}

func TestRemoveNotExists(t *testing.T) {
	db, err := NewFSKV("data")
	assert.Nil(t, err)
	err = db.Remove(randString(10))
	assert.Nil(t, err)
}

func TestRemoveAll(t *testing.T) {
	db, err := NewFSKV("data")
	assert.Nil(t, err)
	err = db.Remove()
	assert.Nil(t, err)
}

func TestRemove(t *testing.T) {
	db, err := NewFSKV("data")
	assert.Nil(t, err)
	key := randString(10)
	err = db.Set(key, []byte(randString(20)))
	assert.Nil(t, err)
	data, err := db.Get(key)
	assert.Nil(t, err)
	assert.NotNil(t, data)
	err = db.Remove(key)
	assert.Nil(t, err)
	data, err = db.Get(key)
	assert.NotNil(t, err)
	assert.Nil(t, data)
}

func TestGetBucket(t *testing.T) {
	db, err := NewFSKV("data")
	assert.Nil(t, err)
	b, err := db.GetBucket(randString(10))
	assert.Nil(t, err)
	b, err = b.GetBucket(randString(10))
	assert.Nil(t, err)
	err = b.Set(randString(10), []byte(randString(10)))
	assert.Nil(t, err)
}

func TestScan(t *testing.T) {
	db, err := NewFSKV(filepath.Join("data", randString(10)))
	assert.Nil(t, err)
	data := make(map[string]string)
	size := rand.Intn(10) + 10

	for i := 0; i < size; i++ {
		key := randString(10)
		value := randString(100)
		data[key] = value
		err := db.Set(key, []byte(value))
		assert.Nil(t, err)
	}

	count := 0

	db.Scan("", func(key string, value []byte) bool {
		count++
		assert.Equal(t, data[key], string(value))
		return true
	})

	assert.Equal(t, size, count)
}

func TestScanPrefix(t *testing.T) {
	prefix := randString(5)
	db, err := NewFSKV(filepath.Join("data", randString(10)))
	assert.Nil(t, err)
	data := make(map[string]string)
	size := rand.Intn(10) + 10
	prefixes := 0

	for i := 0; i < size; i++ {

		var key string

		if i%2 == 0 {
			key += prefix
			prefixes++
		}

		key += randString(10)
		value := randString(100)
		data[key] = value
		err := db.Set(key, []byte(value))
		assert.Nil(t, err)
	}

	count := 0

	db.Scan(prefix, func(key string, value []byte) bool {
		count++
		assert.Equal(t, data[key], string(value))
		return true
	})

	assert.Equal(t, prefixes, count)
}

func TestScanStop(t *testing.T) {
	db, err := NewFSKV(filepath.Join("data", randString(10)))
	assert.Nil(t, err)
	size := rand.Intn(10) + 10

	for i := 0; i < size; i++ {
		err := db.Set(randString(10), []byte(randString(100)))
		assert.Nil(t, err)
	}

	count := 0

	db.Scan("", func(key string, value []byte) bool {
		count++
		return count < 4
	})

	assert.Equal(t, 4, count)
}

func TestBucketInvalidName(t *testing.T) {
	db, err := NewFSKV("data")
	assert.Nil(t, err)
	b, err := db.GetBucket("*\000,:;&&")
	assert.NotNil(t, err)
	assert.Nil(t, b)
}

func TestSetLocked(t *testing.T) {
	db, err := NewFSKV("data")
	assert.Nil(t, err)
	key := randString(10)
	l, err := getLock(db.root.pool.Get(), key)
	assert.Nil(t, err)
	assert.NotNil(t, l)
	err = db.Set(key, []byte(randString(20)))
	assert.NotNil(t, err)
}

func TestRemoveLocked(t *testing.T) {
	db, err := NewFSKV("data")
	assert.Nil(t, err)
	key := randString(10)
	err = db.Set(key, []byte(randString(20)))
	assert.Nil(t, err)
	l, err := getLock(db.root.pool.Get(), key)
	assert.Nil(t, err)
	assert.NotNil(t, l)
	err = db.Remove(key)
	assert.NotNil(t, err)
}

func TestSetInvalidName(t *testing.T) {
	db, err := NewFSKV("data")
	assert.Nil(t, err)
	err = db.Set("*\000,:;&&", []byte(randString(20)))
	assert.NotNil(t, err)
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890")
var letterRunesLen = len(letterRunes)

func randString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(letterRunesLen)]
	}
	return string(b)
}
