package watch

import "log"

type FileChanges struct {
	Modified chan bool // Channel to get notified of modifications
	Rotated  chan bool // Channel to get notified of logrotate
	/*
		Truncated chan bool // Channel to get notified of truncations
		Deleted   chan bool // Channel to get notified of deletions/renames
	*/
}

func NewFileChanges() *FileChanges {
	return &FileChanges{
		Modified: make(chan bool),
		Rotated:  make(chan bool),
	}
}

func (fc *FileChanges) NotifyRotated() {
	sendOnlyIfEmpty(fc.Modified)
}
func (fc *FileChanges) NotifyModified() {
	sendOnlyIfEmpty(fc.Modified)
}

/*
func (fc *FileChanges) NotifyTruncated() {
	sendOnlyIfEmpty(fc.Truncated)
}

func (fc *FileChanges) NotifyDeleted() {
	sendOnlyIfEmpty(fc.Deleted)
}

func (fc *FileChanges) NotifyClosed() {
	sendOnlyIfEmpty(fc.Rotated)
}
*/

func (fc *FileChanges) Close() {
	log.Printf("Close FileChanges.")
	close(fc.Modified)
	close(fc.Rotated)
	/*
		close(fc.Truncated)
		close(fc.Deleted)
	*/
}

// sendOnlyIfEmpty sends on a bool channel only if the channel has no
// backlog to be read by other goroutines. This concurrency pattern
// can be used to notify other goroutines if and only if they are
// looking for it (i.e., subsequent notifications can be compressed
// into one).
func sendOnlyIfEmpty(ch chan bool) {
	select {
	case ch <- true:
	default:
	}
}