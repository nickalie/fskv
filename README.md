# Golang Binary Wrapper

[![](https://img.shields.io/badge/docs-godoc-blue.svg)](https://godoc.org/github.com/nickalie/fskv)
[![](https://circleci.com/gh/nickalie/fskv.png?circle-token=4e9ad77c8463b3a34502ea66d47d35d22bd5eb65)](https://circleci.com/gh/nickalie/fskv)
[![codecov](https://codecov.io/gh/nickalie/fskv/branch/master/graph/badge.svg)](https://codecov.io/gh/nickalie/fskv)

## Install

```go get -u github.com/nickalie/fskv```

## Example of usage

```go
package main

import (
	"github.com/nickalie/fskv"
	"log"
	"fmt"
)

func main()  {
	db, err := fskv.NewFSKV("data")

	if err != nil {
		log.Fatal(err)
	}

	db.Set("mykey", []byte("somevalue"))

	value, _ := db.Get("mykey")

	fmt.Println("Got: " + string(value))
}
```

## Buckets

Buckets are collections of key/value pairs within the storage. You can create any amount of nested buckets

```go
bucket, _ := db.GetBucket("mybucket")
bucket.Set("some_key", []byte("some_value"))
value, _ := bucket.Get("another_key")
childBucket, := bucket.GetBucket("childbucket")
```

## Iterating

To iterate over the keys:

```go
db.Scan("", func(key string, value []byte) bool {
		fmt.Printf("key: %s, value: %s\n", key, string(value))
		return false
	})
```

You can specify *prefix* to iterate over keys with that prefix.
