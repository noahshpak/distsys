package log

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	write = []byte("welcome to the log")
	width = uint64(len(write) + lenWidth)
)

func TestWriteRead(t *testing.T) {
	f, err := ioutil.TempFile("", "TestWriteRead")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)
	testAppend(t, s, 10)
}

func testAppend(t *testing.T, s *store, times uint64) {
	t.Helper()
	for i := uint64(1); i < times; i++ {
		n, pos, err := s.Append(write)
		require.NoError(t, err)
		require.Equal(t, pos+n, width*i)
	}
}
