package ftail

import (
	"bytes"
	"compress/zlib"
	"hash"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/masahide/ftailer/core"
	"github.com/masahide/ftailer/tail"
	"github.com/masahide/ftailer/tailex"
	"golang.org/x/net/context"
)

//const defaultMaxHeadHashSize = 1024

type Config struct {
	Name            string
	BufDir          string
	Period          time.Duration // 分割保存インターバル
	MaxHeadHashSize int64

	tailex.Config
}

type Ftail struct {
	rec       *core.Recorder
	lastSlice time.Time
	Pos       *core.Position
	Config

	buf bytes.Buffer
	*zlib.Writer
	lastTime time.Time
	headHash hash.Hash64
}

var tailDefaultConfig = tail.Config{
	Follow: true,
	ReOpen: true,
	Poll:   false,
	//OpenNotify:  true,
	//MaxLineSize:    16 * 1024 * 1024, // 16MB
	NotifyInterval: 1 * time.Second,
}

// ポジション情報がない場合に実ファイルから取得
func position(c Config) (pos *core.Position, err error) {
	var fi os.FileInfo
	filePath := c.Path
	if c.PathFmt != "" { // cronolog
		timeSlice := tailex.Truncate(c.Config.Time, c.RotatePeriod)
		searchPath := tailex.Time2Path(c.PathFmt, timeSlice)
		filePath, err = tailex.GlobSearch(searchPath)
		if err == tailex.ErrNoSuchFile {
			log.Printf("ftail position() GlobSearch(%s)  err: %s", searchPath, err)
			return &core.Position{}, nil
		} else if err != nil {
			log.Printf("ftail position() GlobSearch(%s)  err: %s", searchPath, err)
			return nil, err
		}
	}
	if fi, err = os.Stat(filePath); err != nil {
		log.Printf("Start os.Stat('%s')  err: %s,  ", filePath, err)
		return nil, err
	}
	pos = &core.Position{
		Name:     filePath,
		CreateAt: fi.ModTime(),
		Offset:   0,
	}
	return
}

func Start(ctx context.Context, c Config) error {
	f := &Ftail{
		Config:   c,
		headHash: fnv.New64(),
	}
	//if f.MaxHeadHashSize == 0 {
	//	f.MaxHeadHashSize = defaultMaxHeadHashSize
	//}
	var err error
	f.rec, err = core.NewRecorder(c.BufDir, c.Name, c.Period)
	if err != nil {
		log.Fatalln("NewRecorder err:", err)
	}
	defer f.rec.AllClose()

	f.Pos = f.rec.Position()
	if f.Pos == nil {
		if f.Pos, err = position(c); err != nil {
			log.Fatalln("position err:", err)
		}
	}
	f.Config.Config.Config = tailDefaultConfig
	f.ReOpenDelay = 5 * time.Second
	if f.Delay != 0 {
		f.ReOpenDelay = f.Delay
	}
	log.Printf("f.Pos: %s", f.Pos)

	if f.MaxHeadHashSize != 0 && f.Pos.Name != "" {
		hash, length, head, err := f.getHeadHash(f.Pos.Name, f.Pos.HashLength)
		if err != nil {
			log.Printf("getHeadHash err:%s", err)
		} else {
			if f.Pos.HeadHash == hash && f.Pos.HashLength == length { // ポジションファイルのハッシュ値と一致した場合はSeekInfoをセット
				log.Printf("match headHash: %s", f.Pos)
				f.Location = &tail.SeekInfo{Offset: f.Pos.Offset}
			} else {
				log.Printf("not match headHash old: %s", f.Pos)
				f.Pos.HeadHash = hash
				f.Pos.HashLength = length
				f.Pos.Head = head
				log.Printf("not match headHash new: %s", f.Pos)
			}
		}
	} else {
		posTimeSlise := tailex.Truncate(f.Pos.CreateAt, c.RotatePeriod)
		nowTimeSlise := tailex.Truncate(time.Now(), c.RotatePeriod)
		if nowTimeSlise.Equal(posTimeSlise) { // 読み込んだポジションのcreateAtが現在のtimesliseと同じ場合
			f.Location = &tail.SeekInfo{Offset: f.Pos.Offset}
		}
	}
	t := tailex.TailFile(ctx, f.Config.Config)
	//var buf bytes.Buffer
	f.buf = bytes.Buffer{}
	f.Writer, err = zlib.NewWriterLevel(&f.buf, zlib.BestCompression)
	if err != nil {
		log.Fatalln("NewZlibWriter err:", err)
	}
	defer f.Flush()

	for {
		select {
		case <-ctx.Done(): // キャンセル処理
			return ctx.Err()
		case line, ok := <-t.Lines: // 新しい入力行の取得
			if !ok {
				return err
			}
			err := f.lineNotifyAction(ctx, line)
			if err != nil {
				return err
			}
		}

	}

}

// lineのNotifyType別に処理を分岐
func (f *Ftail) lineNotifyAction(ctx context.Context, line *tail.Line) error {
	var err error
	switch line.NotifyType {
	case tail.NewLineNotify: // 新しいライン
		err = f.Write(line)
		if err != nil {
			return err
		}
	case tail.TickerNotify, tailex.GlobLoopNotify: // 定期flush処理
		if err := f.Flush(); err != nil {
			return err
		}
		timeSlice := tailex.Truncate(line.Time, f.Period)
		if f.lastSlice.Sub(timeSlice) < 0 {
			// 新しいDBを開く
			if _, err = f.rec.CreateDB(timeSlice, f.Pos); err != nil {
				log.Printf("CreateDB err:%s", err)
				return err
			}
			f.lastSlice = timeSlice
		}
		// 古いDBを閉じる
		if _, err := f.rec.CloseOldDbs(line.Time); err != nil {
			log.Printf("CloseOldDbs err:%s", err)
			return err
		}
	case tail.NewFileNotify:
		f.lastTime = line.Time
		f.Pos.Name = line.Filename
		f.Pos.CreateAt = line.OpenTime
		f.Pos.Offset = line.Offset
		f.Pos.HeadHash, f.Pos.HashLength, f.Pos.Head, err = f.getHeadHash(f.Pos.Name, f.MaxHeadHashSize)
		if err != nil {
			log.Printf("NewFileNotify getHeadHash err:%s", err)
			return err
		}
		log.Printf("NewFileNotify: %v", f.Pos)
	}
	return nil
}

func (f *Ftail) addHash(line []byte) error {
	restSize := f.MaxHeadHashSize - f.Pos.HashLength
	if int64(len(line)) >= restSize {
		line = line[0:restSize]
	}
	written, err := f.headHash.Write(line)
	if err != nil {
		return err
	}
	f.Pos.Head = append(f.Pos.Head, line...)
	f.Pos.HeadHash = strconv.FormatUint(f.headHash.Sum64(), 16)
	f.Pos.HashLength += int64(written)
	log.Printf("addHash %s", f.Pos)
	return nil
}

func (f *Ftail) Write(line *tail.Line) (err error) {
	f.lastTime = line.Time
	f.Pos.Name = line.Filename
	f.Pos.CreateAt = line.OpenTime
	f.Pos.Offset = line.Offset
	if f.Pos.HashLength < f.MaxHeadHashSize {
		if err = f.addHash(line.Text); err != nil {
			return err
		}
	}
	_, err = f.Writer.Write(line.Text)
	return err
}

func (f *Ftail) Flush() error {
	if f.buf.Len() <= 0 {
		return nil
	}
	f.Writer.Close()
	err := f.rec.Put(core.Record{Time: f.lastTime, Data: f.buf.Bytes()}, f.Pos)
	log.Printf("rec.Put: %v", f.Pos)
	f.buf.Reset()
	f.Reset(&f.buf)
	if err != nil {
		log.Printf("Flush %s err:%s", f.Pos.Name, err)
	}
	return err
}

func (f *Ftail) getHeadHash(fname string, getLength int64) (hash string, length int64, head []byte, err error) {
	if f.MaxHeadHashSize == 0 || f.Pos.Name == "" {
		return "", 0, []byte{}, nil
	}
	var readFile *os.File
	readFile, err = os.Open(fname)
	if err != nil {
		return
	}
	defer readFile.Close()
	f.headHash = fnv.New64()
	tee := io.TeeReader(io.LimitReader(readFile, getLength), f.headHash)
	head, err = ioutil.ReadAll(tee)
	length = int64(len(head))
	//length, err = io.CopyN(f.headHash, readFile, getLength)
	switch err {
	case nil:
	case io.EOF:
		err = nil
	default:
		return
	}
	hash = strconv.FormatUint(f.headHash.Sum64(), 16)
	return
}
