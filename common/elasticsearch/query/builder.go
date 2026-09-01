package query

import "encoding/json"
type Builder struct {
	query                 Query         // query
	from                  int           // from
	size                  int           // size
	sorters               []Sorter      // sort
	searchAfterSortValues []interface{} // search_after

}

func NewBuilder() *Builder {
	return &Builder{
		from: -1,
		size: -1,
	}
}

func (b *Builder) Query(query Query) *Builder {
	b.query = query
	return b
}

func (b *Builder) From(from int) *Builder {
	b.from = from
	return b
}

func (b *Builder) Sortby(sorters ...Sorter) *Builder {
	b.sorters = sorters
	return b
}

func (b *Builder) Size(size int) *Builder {
	b.size = size
	return b
}

func (b *Builder) SearchAfter(v ...interface{}) *Builder {
	b.searchAfterSortValues = v
	return b
}

// Source returns the serializable JSON for the source builder.
func (b *Builder) Source() (interface{}, error) {
	source := make(map[string]interface{})

	if b.from != -1 {
		source["from"] = b.from
	}
	if b.size != -1 {
		source["size"] = b.size
	}

	if b.query != nil {
		src, err := b.query.Source()
		if err != nil {
			return nil, err
		}
		source["query"] = src
	}
	if len(b.sorters) > 0 {
		var sortarr []interface{}
		for _, sorter := range b.sorters {
			src, err := sorter.Source()
			if err != nil {
				return nil, err
			}
			sortarr = append(sortarr, src)
		}
		source["sort"] = sortarr
	}

	if len(b.searchAfterSortValues) > 0 {
		source["search_after"] = b.searchAfterSortValues
	}

	return source, nil
}

func (b *Builder) String() (string, error) {
	source, err := b.Source()
	if err != nil {
		return "", err
	}

	marshaled, err := json.Marshal(source)
	if err != nil {
		return "", err
	}

	return string(marshaled), nil
}
