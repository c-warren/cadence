package query

type Query interface {
	// Source returns the JSON-serializable query request.
	Source() (interface{}, error)
}

type BoolQuery struct {
	mustClauses    []Query
	mustNotClauses []Query
	filterClauses  []Query
}

// Creates a new bool query.
func NewBoolQuery() *BoolQuery {
	return &BoolQuery{
		mustClauses:    make([]Query, 0),
		mustNotClauses: make([]Query, 0),
		filterClauses:  make([]Query, 0),
	}
}

func (q *BoolQuery) Must(queries ...Query) *BoolQuery {
	q.mustClauses = append(q.mustClauses, queries...)
	return q
}

func (q *BoolQuery) MustNot(queries ...Query) *BoolQuery {
	q.mustNotClauses = append(q.mustNotClauses, queries...)
	return q
}

func (q *BoolQuery) Filter(filters ...Query) *BoolQuery {
	q.filterClauses = append(q.filterClauses, filters...)
	return q
}

func (q *BoolQuery) Source() (interface{}, error) {
	query := make(map[string]interface{})

	boolClause := make(map[string]interface{})
	query["bool"] = boolClause

	// must
	if len(q.mustClauses) == 1 {
		src, err := q.mustClauses[0].Source()
		if err != nil {
			return nil, err
		}
		boolClause["must"] = src
	} else if len(q.mustClauses) > 1 {
		var clauses []interface{}
		for _, subQuery := range q.mustClauses {
			src, err := subQuery.Source()
			if err != nil {
				return nil, err
			}
			clauses = append(clauses, src)
		}
		boolClause["must"] = clauses
	}

	// must_not
	if len(q.mustNotClauses) == 1 {
		src, err := q.mustNotClauses[0].Source()
		if err != nil {
			return nil, err
		}
		boolClause["must_not"] = src
	} else if len(q.mustNotClauses) > 1 {
		var clauses []interface{}
		for _, subQuery := range q.mustNotClauses {
			src, err := subQuery.Source()
			if err != nil {
				return nil, err
			}
			clauses = append(clauses, src)
		}
		boolClause["must_not"] = clauses
	}

	// filter
	if len(q.filterClauses) == 1 {
		src, err := q.filterClauses[0].Source()
		if err != nil {
			return nil, err
		}
		boolClause["filter"] = src
	} else if len(q.filterClauses) > 1 {
		var clauses []interface{}
		for _, subQuery := range q.filterClauses {
			src, err := subQuery.Source()
			if err != nil {
				return nil, err
			}
			clauses = append(clauses, src)
		}
		boolClause["filter"] = clauses
	}

	return query, nil
}
