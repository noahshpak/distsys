package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var enc = binary.BigEndian

const lenWidth = 8

type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func newStore(f *os.File) (*store, error) {
	fileInfo, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fileInfo.Size())
	return &store{
		File: f,
		buf:  bufio.NewWriter(f),
		size: size,
	}, nil
}

/*
+---------------+---------+
|   Data        |         |
|   written to  |  file   |
|      so far   |         |
+---------------+---------+
<-0---1---2---3---4---5---6---7...-n-n+1-n+2-n+3...-n+k-n+k+1---...>
pos                    lenWidth          actual data       size (EOF)
^
start of data

lenWidth
uint64 representing number of bytes	in actual data
*/
func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pos = s.size
	// write len(p) as BigEndian bytes (8 bytes for uint64)
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}
	// write actual data
	// writing to the buffer instead of directly to the file
	// reduces the number of system calls
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}
	w += lenWidth
	s.size += uint64(w)
	return uint64(w), pos, nil
}

func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	lengthBytes := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(lengthBytes, int64(pos)); err != nil {
		return nil, err
	}

	numStoredBytes := enc.Uint64(lengthBytes)
	data := make([]byte, numStoredBytes)
	if _, err := s.File.ReadAt(data, int64(pos+lenWidth)); err != nil {
		return nil, err
	}
	return data, nil
}

func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return 0, err
	}
	return s.File.ReadAt(p, off)
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return err
	}
	return s.File.Close()

}
