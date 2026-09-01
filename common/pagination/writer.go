package pagination

type (
	writer struct {
		writeFn       WriteFn
		shouldFlushFn ShouldFlushFn
		flushedPages  []PageToken
		page          Page
	}
)

// NewWriter constructs a new Writer
func NewWriter(
	writeFn WriteFn,
	shouldFlushFn ShouldFlushFn,
	startingPage PageToken,
) Writer {
	return &writer{
		writeFn:       writeFn,
		shouldFlushFn: shouldFlushFn,
		flushedPages:  nil,
		page: Page{
			Entities:     nil,
			CurrentToken: startingPage,
		},
	}
}

// Add adds entity to buffer and flushes if provided shouldFlushFn indicates the page should be flushed.
func (w *writer) Add(e Entity) error {
	w.page.Entities = append(w.page.Entities, e)
	if !w.shouldFlushFn(w.page) {
		return nil
	}
	return w.Flush()
}

// Flush flushes the buffer.
func (w *writer) Flush() error {
	nextPageToken, err := w.writeFn(w.page)
	if err != nil {
		return err
	}
	w.flushedPages = append(w.flushedPages, w.page.CurrentToken)
	w.page = Page{
		Entities:     nil,
		CurrentToken: nextPageToken,
	}
	return nil
}

// FlushIfNotEmpty flushes the buffer if and only if it is not empty
func (w *writer) FlushIfNotEmpty() error {
	if len(w.page.Entities) == 0 {
		return nil
	}
	return w.Flush()
}

// FlushedPages returns all pages which have been successfully flushed.
func (w *writer) FlushedPages() []PageToken {
	return w.flushedPages
}

// FirstFlushedPage returns the first page that was flushed or nil if no pages have been flushed.
func (w *writer) FirstFlushedPage() PageToken {
	if len(w.flushedPages) == 0 {
		return nil
	}
	return w.flushedPages[0]
}

// LastFlushedPage returns the last page that was flushed or nil if no pages have been flushed
func (w *writer) LastFlushedPage() PageToken {
	if len(w.flushedPages) == 0 {
		return nil
	}
	return w.flushedPages[len(w.flushedPages)-1]
}
