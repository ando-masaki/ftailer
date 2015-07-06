package main

import (
	"flag"
	"io"
	"log"
	"os"
	"time"

	"github.com/boltdb/bolt"
	"github.com/masahide/ftailer/core"
)

type Config struct {
	BufDir string
	Name   string
	Period time.Duration // time.Minute
}

var config = Config{
	BufDir: "testbuf",
	Name:   "test.log",
	Period: 1 * time.Minute,
}

func main() {

	flag.StringVar(&config.Name, "name", config.Name, "logfile")
	flag.StringVar(&config.BufDir, "bufdir", config.BufDir, "BufDir path")
	flag.Parse()

	db := &core.DB{Path: config.BufDir, Name: config.Name}
	// fixed fileを検索
	dbfiles, err := core.FixGlob(db)
	if err != nil {
		log.Printf("find err:%s", err)
		return
	}
	if len(dbfiles) == 0 {
		log.Printf("not such file :%s", config.BufDir)
		return
	}
	for _, f := range dbfiles {
		db.Time = f.Time
		if err = db.Open(core.FixExt); err != nil {
			log.Printf("not found db: %s", f.Path)
			return
		}
		log.Printf("open db: %v -------------", f)
		readDb(os.Stdout, db)
		db.Close(false)

	}
}

func readDb(w io.Writer, db *core.DB) error {
	return db.View(func(tx *bolt.Tx) error {
		c := core.Cursor(tx)
		for k, v := c.First(); k != nil; k, v = c.Next() {
			r, err := core.ReadRecord(k, v)
			if err != nil {
				return err
			}
			_, err = io.Copy(w, r)
			if err != nil {
				r.Close()
				return err
			}
			r.Close()
		}
		return nil
	})
}