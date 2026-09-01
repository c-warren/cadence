package types

import (
	"encoding/json"
	"fmt"
)

type DispatchPolicy struct {
	// DispatchRPS is the rate limit for items dequeued from the node to be pushed to processors.
	// All nodes inherit the DispatchRPS from the parent node as is (not distributed to children).
	// If parent has 100 rps limit, then all curent and to-be-created children will have 100 rps limit.
	DispatchRPS int64 `json:"dispatchRPS,omitempty"`

	// Concurrency is the maximum number of items to be processed concurrently.
	Concurrency int `json:"concurrency,omitempty"`

	// TODO: define retry policy
}

func (dp DispatchPolicy) String() string {
	return fmt.Sprintf("DispatchPolicy{DispatchRPS:%d, Concurrency:%d}", dp.DispatchRPS, dp.Concurrency)
}

type SplitPolicy struct {
	// Disabled is used to disable the split policy for the node.
	Disabled bool `json:"disabled,omitempty"`

	// PredefinedSplits is a list of predefined splits for the attribute key
	// Child nodes for these attributes will be created during initialization
	PredefinedSplits []any `json:"predefinedSplits,omitempty"`
}

func (sp SplitPolicy) String() string {
	return fmt.Sprintf("SplitPolicy{Disabled:%v, PredefinedSplits:%v}", sp.Disabled, sp.PredefinedSplits)
}

type NodePolicy struct {
	// The path to the node
	// Root node has empty path "".
	// "/" is used as path separator.
	// "*" means the policy applies to the special catch-all node
	// "." means the policy applies to all nodes in the specified level except the catch-all node
	Path string `json:"path,omitempty"`

	SplitPolicy *SplitPolicy `json:"splitPolicy,omitempty"`

	// DispatchPolicy is enforced at the leaf node level.
	DispatchPolicy *DispatchPolicy `json:"dispatchPolicy,omitempty"`
}

// Merge merges two NodePolicy objects by marshalling/unmarshalling them.
// Any field in the other policy will override the field in the current policy.
func (np NodePolicy) Merge(other NodePolicy) (NodePolicy, error) {
	marshalled1, err := json.Marshal(np)
	if err != nil {
		return NodePolicy{}, err
	}

	var m1 map[string]any
	err = json.Unmarshal(marshalled1, &m1)
	if err != nil {
		return NodePolicy{}, err
	}

	marshalled2, err := json.Marshal(other)
	if err != nil {
		return NodePolicy{}, err
	}

	var m2 map[string]any
	err = json.Unmarshal(marshalled2, &m2)
	if err != nil {
		return NodePolicy{}, err
	}

	for k, v2 := range m2 {
		m1[k] = v2
	}

	mergedMarshalled, err := json.Marshal(m1)
	if err != nil {
		return NodePolicy{}, err
	}

	var merged NodePolicy
	err = json.Unmarshal(mergedMarshalled, &merged)
	if err != nil {
		return NodePolicy{}, err
	}

	return merged, nil
}

func (np NodePolicy) String() string {
	return fmt.Sprintf("NodePolicy{Path:%v, DispatchPolicy:%s, SplitPolicy:%s}", np.Path, np.DispatchPolicy, np.SplitPolicy)
}
