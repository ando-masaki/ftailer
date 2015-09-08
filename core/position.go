package core

import (
	"fmt"
	"strconv"
	"time"

	"github.com/boltdb/bolt"
)

const (
	posBucketName = "position"
)

type Position struct {
	Name       string
	CreateAt   time.Time
	Offset     int64
	HeadHash   string
	Head       []byte
	HashLength int64
}

func (p Position) String() string {
	return fmt.Sprintf("Name:%s, CreateAt:%s, Offset:%d, HeadHash:%s, hashLen:%d, head:%s", p.Name, p.CreateAt, p.Offset, p.HeadHash, p.HashLength, p.Head)

}

func createPosBucket(tx *bolt.Tx) error {
	_, err := tx.CreateBucketIfNotExists([]byte(posBucketName))
	return err
}

func (p Position) Put(tx *bolt.Tx) error {
	var err error
	b := tx.Bucket([]byte(posBucketName))
	if err = b.Put([]byte("Name"), []byte(p.Name)); err != nil {
		return err
	}
	if err = b.Put([]byte("CreateAt"), []byte(p.CreateAt.Format(time.RFC3339))); err != nil {
		return err
	}
	if err = b.Put([]byte("Offset"), []byte(strconv.FormatInt(p.Offset, 16))); err != nil {
		return err
	}
	if err = b.Put([]byte("HeadHash"), []byte(p.HeadHash)); err != nil {
		return err
	}
	if err = b.Put([]byte("Head"), p.Head); err != nil {
		return err
	}
	if err = b.Put([]byte("HashLength"), []byte(strconv.FormatInt(p.HashLength, 16))); err != nil {
		return err
	}
	return nil
}

func GetPositon(tx *bolt.Tx) (Position, error) {
	var err error
	var p Position
	var value []byte
	b := tx.Bucket([]byte(posBucketName))
	if value = b.Get([]byte("Name")); value == nil {
		return p, ErrNotFound
	}
	p.Name = string(value)
	if value = b.Get([]byte("CreateAt")); value == nil {
		return p, ErrNotFound
	}
	if p.CreateAt, err = time.Parse(time.RFC3339, string(value)); err != nil {
		return p, err
	}
	if value = b.Get([]byte("Offset")); value == nil {
		return p, ErrNotFound
	}
	if p.Offset, err = strconv.ParseInt(string(value), 16, 64); err != nil {
		return p, err
	}
	if value = b.Get([]byte("HeadHash")); value != nil {
		p.HeadHash = string(value)
	}
	if value = b.Get([]byte("Head")); value != nil {
		p.Head = value
	}
	if value = b.Get([]byte("HashLength")); value != nil {
		p.HashLength, _ = strconv.ParseInt(string(value), 16, 64)
	}
	return p, nil
}
