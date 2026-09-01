package pagination

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type WriterSuite struct {
	*require.Assertions
	suite.Suite
}

func TestWriterSuite(t *testing.T) {
	suite.Run(t, new(WriterSuite))
}

func (s *WriterSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *WriterSuite) TestAddNoFlush() {
	writeFn := func(_ Page) (PageToken, error) {
		return nil, nil
	}
	shouldFlushFn := func(_ Page) bool {
		return false
	}
	writer := NewWriter(writeFn, shouldFlushFn, nil)
	s.NoError(writer.Add(1))
	s.Empty(writer.FlushedPages())
	s.Nil(writer.FirstFlushedPage())
	s.Nil(writer.LastFlushedPage())
}

func (s *WriterSuite) TestAddAndFlush() {
	writeFn := func(_ Page) (PageToken, error) {
		return nil, nil
	}
	shouldFlushFn := func(_ Page) bool {
		return true
	}
	writer := NewWriter(writeFn, shouldFlushFn, "test_token")
	s.NoError(writer.Add(1))
	flushedPages := writer.FlushedPages()
	s.Len(flushedPages, 1)
	s.Equal("test_token", flushedPages[0].(string))
	s.Equal("test_token", writer.FirstFlushedPage().(string))
	s.Equal("test_token", writer.LastFlushedPage().(string))
}

func (s *WriterSuite) TestFlushErrorOnWrite() {
	writeFn := func(_ Page) (PageToken, error) {
		return nil, errors.New("got error")
	}
	shouldFlushFn := func(_ Page) bool {
		return true
	}
	writer := NewWriter(writeFn, shouldFlushFn, nil)
	s.Error(writer.Flush())
}

func (s *WriterSuite) TestFlushNoError() {
	writeFn := func(_ Page) (PageToken, error) {
		return nil, nil
	}
	shouldFlushFn := func(_ Page) bool {
		return true
	}
	writer := NewWriter(writeFn, shouldFlushFn, "test_token")
	s.Len(writer.FlushedPages(), 0)
	s.NoError(writer.Flush())
	s.Len(writer.FlushedPages(), 1)
	s.Equal("test_token", writer.FlushedPages()[0].(string))
	s.Equal("test_token", writer.FirstFlushedPage().(string))
	s.Equal("test_token", writer.LastFlushedPage().(string))
	s.NoError(writer.FlushIfNotEmpty())
	s.Len(writer.FlushedPages(), 1)
	s.Equal("test_token", writer.FlushedPages()[0].(string))
	s.Equal("test_token", writer.FirstFlushedPage().(string))
	s.Equal("test_token", writer.LastFlushedPage().(string))
}

func (s *WriterSuite) TestMultiPageWrite() {
	pageNum := 0
	writeFnCalls := 0
	writeFn := func(_ Page) (PageToken, error) {
		writeFnCalls++
		pageNum++
		return fmt.Sprintf("page_%v", pageNum), nil
	}
	shouldFlushCalls := 0
	shouldFlushFn := func(p Page) bool {
		shouldFlushCalls++
		return len(p.Entities) == 5
	}
	writer := NewWriter(writeFn, shouldFlushFn, "page_0")
	for i := 1; i <= 100; i++ {
		s.NoError(writer.Add(i))
	}
	var expectedPages []string
	for i := 0; i < 20; i++ {
		expectedPages = append(expectedPages, fmt.Sprintf("page_%v", i))
	}
	actualPages := writer.FlushedPages()
	s.Len(actualPages, 20)
	for i, expected := range expectedPages {
		s.Equal(expected, actualPages[i].(string))
	}
	s.Equal(writeFnCalls, 20)
	s.Equal(shouldFlushCalls, 100)
	s.Equal("page_0", writer.FirstFlushedPage().(string))
	s.Equal("page_19", writer.LastFlushedPage().(string))

}
