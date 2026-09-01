package pagination

import "context"

type (
	iterator struct {
		ctx context.Context

		page        Page
		entityIndex int

		nextEntity Entity
		nextError  error

		fetchFn FetchFn
	}
)

// NewIterator constructs a new Iterator
func NewIterator(
	ctx context.Context,
	startingPageToken PageToken,
	fetchFn FetchFn,
) Iterator {
	itr := &iterator{
		ctx: ctx,
		page: Page{
			Entities:     nil,
			CurrentToken: nil,
			NextToken:    startingPageToken,
		},
		entityIndex: 0,
		fetchFn:     fetchFn,
	}
	itr.advance(true)
	return itr
}

// Next returns the next Entity or error.
// Returning nil, nil is valid if that is what the provided fetch function provided.
func (i *iterator) Next() (Entity, error) {
	entity := i.nextEntity
	err := i.nextError
	i.advance(false)
	return entity, err
}

// HasNext returns true if there is a next element. There is considered to be a next element
// As long as a fatal error has not occurred and the iterator has not reached the end.
func (i *iterator) HasNext() bool {
	return i.nextError == nil
}

func (i *iterator) advance(firstPage bool) {
	if !i.HasNext() && !firstPage {
		return
	}
	if i.entityIndex < len(i.page.Entities) {
		i.consume()
	} else {
		if err := i.advanceToNonEmptyPage(firstPage); err != nil {
			i.terminate(err)
		} else {
			i.consume()
		}
	}
}

func (i *iterator) advanceToNonEmptyPage(firstPage bool) error {
	if i.page.NextToken == nil && !firstPage {
		return ErrIteratorFinished
	}
	nextPage, err := i.fetchFn(i.ctx, i.page.NextToken)
	if err != nil {
		return err
	}
	i.page = nextPage
	if len(i.page.Entities) != 0 {
		i.entityIndex = 0
		return nil
	}
	return i.advanceToNonEmptyPage(false)
}

func (i *iterator) consume() {
	i.nextEntity = i.page.Entities[i.entityIndex]
	i.nextError = nil
	i.entityIndex++
}

func (i *iterator) terminate(err error) {
	i.nextEntity = nil
	i.nextError = err
}
