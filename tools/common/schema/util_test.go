package schema

import (
	"io"
	"io/fs"
	"testing"

	"github.com/stretchr/testify/assert"
)

const testdata = `
ALTER TYPE domain_config ADD isolation_groups blob;
ALTER TYPE domain_config ADD isolation_groups_encoding text;
-- a comment
ALTER TYPE domain_config2 ADD isolation_groups_encoding text;
`

type mockfile struct {
	read bool
}

func (m *mockfile) Stat() (fs.FileInfo, error) {
	return nil, nil
}
func (m *mockfile) Read(in []byte) (int, error) {
	if m.read {
		return 0, io.EOF
	}
	for i := 0; i < len(in) && i < len([]byte(testdata)); i++ {
		in[i] = []byte(testdata)[i]
	}
	m.read = true
	return len(in), nil
}
func (m *mockfile) Close() error {
	return nil
}

func TestParseFile(t *testing.T) {
	res, err := ParseFile(&mockfile{})
	assert.NoError(t, err)
	expectedOutput := []string{
		"ALTER TYPE domain_config ADD isolation_groups blob;",
		"ALTER TYPE domain_config ADD isolation_groups_encoding text;",
		"ALTER TYPE domain_config2 ADD isolation_groups_encoding text;",
	}
	assert.Equal(t, expectedOutput, res)
}
