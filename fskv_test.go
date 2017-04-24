package fskv

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"math/rand"
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