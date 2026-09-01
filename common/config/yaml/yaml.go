// Package yaml provides a lazy YAML unmarshaler that defers decoding until
// the eventual target type is known. It has no dependencies beyond yaml.v2,
// so it can be imported by leaf config packages (e.g.
// common/dynamicconfig/openfeatureclient/config) that must stay
// import-cycle-free with respect to common/config.
package yaml

import (
	"fmt"

	yamlv2 "gopkg.in/yaml.v2" // CAUTION: go.uber.org/config does not support yaml.v3
)

// Node is a lazy-unmarshaler, because *yaml.Node only exists in gopkg.in/yaml.v3, not v2,
// and go.uber.org/config currently uses only v2.
type Node struct {
	unmarshal func(out any) error
}

var _ yamlv2.Unmarshaler = (*Node)(nil)

func (n *Node) UnmarshalYAML(unmarshal func(interface{}) error) error {
	n.unmarshal = unmarshal
	return nil
}

func (n *Node) Decode(out any) error {
	if n == nil {
		return nil
	}
	return n.unmarshal(out)
}

// ToNode is a bit of a hack to get a *yaml.Node for config-parsing compatibility purposes.
// There is probably a better way to achieve this with yaml-loading compatibility, but this is at least fairly simple.
func ToNode(input any) (*Node, error) {
	data, err := yamlv2.Marshal(input)
	if err != nil {
		// should be extremely unlikely, unless yaml marshaling is customized
		return nil, fmt.Errorf("could not serialize data to yaml: %w", err)
	}
	var out *Node
	err = yamlv2.Unmarshal(data, &out)
	if err != nil {
		// should not be possible
		return nil, fmt.Errorf("could not deserialize to yaml node: %w", err)
	}
	return out, nil
}
