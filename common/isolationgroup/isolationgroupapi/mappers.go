package isolationgroupapi

import (
	"encoding/json"
	"fmt"

	"github.com/uber/cadence/common/types"
)

func MapDynamicConfigResponse(in []interface{}) (out types.IsolationGroupConfiguration, err error) {
	if in == nil {
		return nil, nil
	}

	out = make(types.IsolationGroupConfiguration, len(in))
	for _, v := range in {
		v1, ok := v.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("failed parse a dynamic config entry, %v, (got %v)", v1, v)
		}
		n, okName := v1["Name"]
		s, okState := v1["State"]
		if !okState || !okName {
			return nil, fmt.Errorf("failed parse a dynamic config entry, %v, (got %v)", v1, v)
		}
		nS, okStr := n.(string)
		sI, okI := s.(float64)
		if !okStr || !okI {
			return nil, fmt.Errorf("failed parse a dynamic config entry, %v, (got %v)", v1, v)
		}
		out[nS] = types.IsolationGroupPartition{
			Name:  nS,
			State: types.IsolationGroupState(sI),
		}
	}
	return out, nil
}

func MapUpdateGlobalIsolationGroupsRequest(in types.IsolationGroupConfiguration) ([]*types.DynamicConfigValue, error) {
	jsonData, err := json.Marshal(in.ToPartitionList())
	if err != nil {
		return nil, fmt.Errorf("failed to marshal input for dynamic config: %w", err)
	}
	out := []*types.DynamicConfigValue{
		&types.DynamicConfigValue{
			Value: &types.DataBlob{
				EncodingType: types.EncodingTypeJSON.Ptr(),
				Data:         jsonData,
			},
		},
	}
	return out, nil
}

func MapAllIsolationGroupsResponse(in []interface{}) ([]string, error) {
	var allIsolationGroups []string
	for k := range in {
		v, ok := in[k].(string)
		if !ok {
			return nil, fmt.Errorf("failed to get all-isolation-groups response from dynamic config: got %v (%T)", in[k], in[k])
		}
		allIsolationGroups = append(allIsolationGroups, v)
	}
	return allIsolationGroups, nil
}
