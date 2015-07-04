package core

import (
	"log"
	"os"
	"path"
	"time"

	"github.com/boltdb/bolt"
)

const (
	recExt = ".rec"
	FixExt = ".fixed"
	delay  = 10 * time.Second
)

type DB struct {
	Path string
	Name string
	Time time.Time

	*bolt.DB
	fix bool
}

func (d *DB) put(record Record, pos *Position) error {
	err := d.Update(func(tx *bolt.Tx) error {
		if err := record.Put(tx, pos, true); err != nil {
			return err
		}
		return pos.Put(tx)
	})
	return err
}

func (d *DB) GetPositon() (pos Position, err error) {
	err = d.View(func(tx *bolt.Tx) error {
		pos, err = GetPositon(tx)
		return err
	})
	return pos, err
}

func (db *DB) createDB(ext string) error {
	if db.DB != nil {
		return nil
	}
	filePath := db.makeFilePath()
	os.MkdirAll(filePath, 0755)
	fp := path.Join(filePath, db.makeFileName())
	var err error
	db.DB, err = bolt.Open(fp+ext, 0644, nil)
	if err != nil {
		db.DB = nil
		return err
	}
	err = db.DB.Update(func(tx *bolt.Tx) error {
		// Create a bucket.
		if err = createRecordBucket(tx); err != nil {
			return err
		}
		if err = createPosBucket(tx); err != nil {
			return err
		}
		return nil
	})
	log.Printf("DB was created. %s", fp+ext)
	return err
}

func (db *DB) Open(ext string) error {
	if db.DB != nil {
		return nil
	}
	fp := path.Join(db.makeFilePath(), db.makeFileName())
	var err error
	db.DB, err = bolt.Open(fp+ext, 0644, nil)
	if err != nil {
		db.DB = nil
	}
	log.Printf("DB was opened.  %s", fp+ext)
	return err
}
func (db *DB) Close(fix bool) error {
	if db.DB == nil {
		return nil
	}
	if fix {
		db.fix = true
	}
	if err := db.DB.Close(); err != nil {
		return err
	}
	db.DB = nil
	if db.fix {
		// mv recExt FixExt
		fileName := path.Join(db.makeFilePath(), db.makeFileName())
		if err := os.Rename(fileName+recExt, fileName+FixExt); err != nil {
			return err
		}
		log.Printf("DB was closed.  %s -> %s", fileName+recExt, fileName+FixExt)
	} else {
		log.Printf("DB was closed.  %s", path.Join(db.makeFilePath(), db.makeFileName())+recExt)
	}
	return nil
}

func makeFilePath(filePath, fileName string, t time.Time) string {
	return path.Join(filePath, fileName, t.Format("20060102"))
}
func makeFileName(t time.Time) string {
	return t.Format("150405")
}

func (db *DB) makeFilePath() string {
	return makeFilePath(db.Path, db.Name, db.Time)
}
func (db *DB) makeFileName() string {
	return makeFileName(db.Time)
}
