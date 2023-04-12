package server

import (
	"fmt"
	"sync"
)

type Log struct {
	mu      sync.Mutex
	records []Record
}

type Offset uint64
type Record struct {
	Value  []byte `json:"value"`
	Offset Offset `json:"offset"`
}

// write the Record struct as a protobuf message

func NewLog() *Log {
	return &Log{}
}

func (c *Log) Append(record Record) (Offset, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	record.Offset = Offset(len(c.records))
	c.records = append(c.records, record)
	return record.Offset, nil
}

var ErrOffsetNotFound = fmt.Errorf("no offset")

func (c *Log) Read(offset Offset) (Record, error) {
	/* do we need a lock on read?
	locking the mutex is not necessary "records" slice is read-only during a read operation
	the "Read" function simply returns a copy of the requested record, and does not modify the log in any way
	there is no race condition to prevent, and the use of a mutex is not necessary for reads.
	*/
	if offset >= Offset(len(c.records)) || offset < Offset(0) {
		return Record{}, ErrOffsetNotFound
	}
	return c.records[offset], nil
}
