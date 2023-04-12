package log

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	write = []byte("welcome to the log")
	width = uint64(len(write) + lenWidth)
	times = uint64(10)
)

func TestWriteRead(t *testing.T) {
	f, err := os.CreateTemp("", "TestWriteRead")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)
	testAppend(t, s)
	testRead(t, s)
	testReadAt(t, s)

	s, err = newStore(f)
	require.NoError(t, err)
	testRead(t, s)
}

// helpers cant be exported
func testAppend(t *testing.T, s *store) {
	t.Helper()
	for i := uint64(1); i < times; i++ {
		n, pos, err := s.Append(write)
		require.NoError(t, err)
		require.Equal(t, pos+n, width*i)
	}
}

func testRead(t *testing.T, s *store) {
	t.Helper()
	var pos uint64
	for i := uint64(1); i < times; i++ {
		d, err := s.Read(pos)
		require.NoError(t, err)
		require.Equal(t, write, d)
		pos += width
	}
}

func testReadAt(t *testing.T, s *store) {
	t.Helper()
	for i := uint64(0); i < times-1; i++ {
		pos := i * width
		size := make([]byte, lenWidth)
		_, err := s.ReadAt(size, int64(pos))
		require.NoError(t, err)
		require.Equal(t, int(enc.Uint64(size)), len(write))
		data := make([]byte, len(write))
		_, err = s.ReadAt(data, int64(pos+lenWidth))
		require.NoError(t, err)
		require.Equal(t, data, write)
	}
}
