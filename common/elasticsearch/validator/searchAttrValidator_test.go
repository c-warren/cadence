package validator

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/common/definition"
	"github.com/uber/cadence/common/dynamicconfig/dynamicproperties"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/types"
)

type searchAttributesValidatorSuite struct {
	suite.Suite
}

func TestSearchAttributesValidatorSuite(t *testing.T) {
	s := new(searchAttributesValidatorSuite)
	suite.Run(t, s)
}

func (s *searchAttributesValidatorSuite) TestValidateSearchAttributes() {
	numOfKeysLimit := 2
	sizeOfValueLimit := 5
	sizeOfTotalLimit := 20

	validator := NewSearchAttributesValidator(log.NewNoop(),
		dynamicproperties.GetBoolPropertyFn(true),
		dynamicproperties.GetMapPropertyFn(definition.GetDefaultIndexedKeys()),
		dynamicproperties.GetIntPropertyFilteredByDomain(numOfKeysLimit),
		dynamicproperties.GetIntPropertyFilteredByDomain(sizeOfValueLimit),
		dynamicproperties.GetIntPropertyFilteredByDomain(sizeOfTotalLimit))

	domain := "domain"
	var attr *types.SearchAttributes

	err := validator.ValidateSearchAttributes(attr, domain)
	s.Nil(err)

	fields := map[string][]byte{
		"CustomIntField": []byte(`1`),
	}
	attr = &types.SearchAttributes{
		IndexedFields: fields,
	}
	err = validator.ValidateSearchAttributes(attr, domain)
	s.Nil(err)

	fields = map[string][]byte{
		"CustomIntField":     []byte(`1`),
		"CustomKeywordField": []byte(`"keyword"`),
		"CustomBoolField":    []byte(`true`),
	}
	attr.IndexedFields = fields
	err = validator.ValidateSearchAttributes(attr, domain)
	s.Equal("number of keys 3 exceed limit", err.Error())

	fields = map[string][]byte{
		"InvalidKey": []byte(`"1"`),
	}
	attr.IndexedFields = fields
	err = validator.ValidateSearchAttributes(attr, domain)
	s.Equal(`InvalidKey is not a valid search attribute key`, err.Error())

	fields = map[string][]byte{
		"CustomStringField": []byte(`"1"`),
		"CustomBoolField":   []byte(`123`),
	}
	attr.IndexedFields = fields
	err = validator.ValidateSearchAttributes(attr, domain)
	s.Equal(`123 is not a valid search attribute value for key CustomBoolField`, err.Error())

	fields = map[string][]byte{
		"CustomIntField": []byte(`[1,2]`),
	}
	attr.IndexedFields = fields
	err = validator.ValidateSearchAttributes(attr, domain)
	s.NoError(err)

	fields = map[string][]byte{
		"StartTime": []byte(`1`),
	}
	attr.IndexedFields = fields
	err = validator.ValidateSearchAttributes(attr, domain)
	s.Equal(`StartTime is read-only Cadence reserved attribute`, err.Error())

	fields = map[string][]byte{
		"CustomKeywordField": []byte(`"123456"`),
	}
	attr.IndexedFields = fields
	err = validator.ValidateSearchAttributes(attr, domain)
	s.Equal(`size limit exceed for key CustomKeywordField`, err.Error())

	fields = map[string][]byte{
		"CustomKeywordField": []byte(`"123"`),
		"CustomStringField":  []byte(`"12"`),
	}
	attr.IndexedFields = fields
	err = validator.ValidateSearchAttributes(attr, domain)
	s.Equal(`total size 44 exceed limit`, err.Error())
}
