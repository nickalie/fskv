package fskv

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"time"
	"path/filepath"
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

func TestRemoveNotExists(t *testing.T)  {
	db, err := NewFSKV("data")
	assert.Nil(t, err)
	err = db.Remove(randString(10))
	assert.Nil(t, err)
}

func TestRemoveAll(t *testing.T)  {
	db, err := NewFSKV("data")
	assert.Nil(t, err)
	err = db.Remove()
	assert.Nil(t, err)
}

func TestRemove(t *testing.T)  {
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

func TestScan(t *testing.T)  {
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