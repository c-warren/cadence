package validator

import (
	"errors"
	"fmt"
	"strings"

	"github.com/xwb1989/sqlparser"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/definition"
	"github.com/uber/cadence/common/dynamicconfig/dynamicproperties"
	"github.com/uber/cadence/common/types"
)

// VisibilityQueryValidator for sql query validation
type VisibilityQueryValidator struct {
	validSearchAttributes          dynamicproperties.MapPropertyFn
	enableQueryAttributeValidation dynamicproperties.BoolPropertyFn
}

// NewQueryValidator create VisibilityQueryValidator
func NewQueryValidator(
	validSearchAttributes dynamicproperties.MapPropertyFn,
	enableQueryAttributeValidation dynamicproperties.BoolPropertyFn) *VisibilityQueryValidator {
	return &VisibilityQueryValidator{
		validSearchAttributes:          validSearchAttributes,
		enableQueryAttributeValidation: enableQueryAttributeValidation,
	}
}

// ValidateQuery validates that search attributes in the query are legal.
// Adds attr prefix for customized fields and returns modified query.
func (qv *VisibilityQueryValidator) ValidateQuery(whereClause string) (string, error) {
	if len(whereClause) != 0 {
		// Build a placeholder query that allows us to easily parse the contents of the where clause.
		// IMPORTANT: This query is never executed, it is just used to parse and validate whereClause
		var placeholderQuery string
		whereClause := strings.TrimSpace(whereClause)
		// #nosec
		if common.IsJustOrderByClause(whereClause) { // just order by
			placeholderQuery = fmt.Sprintf("SELECT * FROM dummy %s", whereClause)
		} else {
			placeholderQuery = fmt.Sprintf("SELECT * FROM dummy WHERE %s", whereClause)
		}

		stmt, err := sqlparser.Parse(placeholderQuery)
		if err != nil {
			return "", &types.BadRequestError{Message: "Invalid query."}
		}

		sel, ok := stmt.(*sqlparser.Select)
		if !ok {
			return "", &types.BadRequestError{Message: "Invalid select query."}
		}
		buf := sqlparser.NewTrackedBuffer(nil)
		// validate where expr
		if sel.Where != nil {
			err = qv.validateWhereExpr(sel.Where.Expr)
			if err != nil {
				return "", &types.BadRequestError{Message: err.Error()}
			}
			sel.Where.Expr.Format(buf)
		}
		// validate order by
		err = qv.validateOrderByExpr(sel.OrderBy)
		if err != nil {
			return "", &types.BadRequestError{Message: err.Error()}
		}
		sel.OrderBy.Format(buf)

		return buf.String(), nil
	}
	return whereClause, nil
}

func (qv *VisibilityQueryValidator) validateWhereExpr(expr sqlparser.Expr) error {
	if expr == nil {
		return nil
	}

	switch expr := expr.(type) {
	case *sqlparser.AndExpr, *sqlparser.OrExpr:
		return qv.validateAndOrExpr(expr)
	case *sqlparser.ComparisonExpr:
		return qv.validateComparisonExpr(expr)
	case *sqlparser.RangeCond:
		return qv.validateRangeExpr(expr)
	case *sqlparser.ParenExpr:
		return qv.validateWhereExpr(expr.Expr)
	default:
		return errors.New("invalid where clause")
	}

}

func (qv *VisibilityQueryValidator) validateAndOrExpr(expr sqlparser.Expr) error {
	var leftExpr sqlparser.Expr
	var rightExpr sqlparser.Expr

	switch expr := expr.(type) {
	case *sqlparser.AndExpr:
		leftExpr = expr.Left
		rightExpr = expr.Right
	case *sqlparser.OrExpr:
		leftExpr = expr.Left
		rightExpr = expr.Right
	}

	if err := qv.validateWhereExpr(leftExpr); err != nil {
		return err
	}
	return qv.validateWhereExpr(rightExpr)
}

func (qv *VisibilityQueryValidator) validateComparisonExpr(expr sqlparser.Expr) error {
	comparisonExpr := expr.(*sqlparser.ComparisonExpr)
	colName, ok := comparisonExpr.Left.(*sqlparser.ColName)
	if !ok {
		return errors.New("invalid comparison expression")
	}
	colNameStr := colName.Name.String()
	if !qv.isValidSearchAttributes(colNameStr) {
		return fmt.Errorf("invalid search attribute %q", colNameStr)
	}

	if !definition.IsSystemIndexedKey(colNameStr) { // add search attribute prefix
		comparisonExpr.Left = &sqlparser.ColName{
			Metadata:  colName.Metadata,
			Name:      sqlparser.NewColIdent(definition.Attr + "." + colNameStr),
			Qualifier: colName.Qualifier,
		}
	}

	return nil
}

func (qv *VisibilityQueryValidator) validateRangeExpr(expr sqlparser.Expr) error {
	rangeCond := expr.(*sqlparser.RangeCond)
	colName, ok := rangeCond.Left.(*sqlparser.ColName)
	if !ok {
		return errors.New("invalid range expression")
	}
	colNameStr := colName.Name.String()

	if !qv.isValidSearchAttributes(colNameStr) {
		return fmt.Errorf("invalid search attribute %q", colNameStr)
	}

	if !definition.IsSystemIndexedKey(colNameStr) { // add search attribute prefix
		rangeCond.Left = &sqlparser.ColName{
			Metadata:  colName.Metadata,
			Name:      sqlparser.NewColIdent(definition.Attr + "." + colNameStr),
			Qualifier: colName.Qualifier,
		}
	}

	return nil
}

func (qv *VisibilityQueryValidator) validateOrderByExpr(orderBy sqlparser.OrderBy) error {
	for _, orderByExpr := range orderBy {
		colName, ok := orderByExpr.Expr.(*sqlparser.ColName)
		if !ok {
			return errors.New("invalid order by expression")
		}
		colNameStr := colName.Name.String()
		if qv.isValidSearchAttributes(colNameStr) {
			if !definition.IsSystemIndexedKey(colNameStr) { // add search attribute prefix
				orderByExpr.Expr = &sqlparser.ColName{
					Metadata:  colName.Metadata,
					Name:      sqlparser.NewColIdent(definition.Attr + "." + colNameStr),
					Qualifier: colName.Qualifier,
				}
			}
		} else {
			return errors.New("invalid order by attribute")
		}
	}
	return nil
}

// isValidSearchAttributes return true if key is registered
func (qv *VisibilityQueryValidator) isValidSearchAttributes(key string) bool {
	if qv.enableQueryAttributeValidation() {
		validAttr := qv.validSearchAttributes()
		_, isValidKey := validAttr[key]
		return isValidKey
	}
	return true
}
