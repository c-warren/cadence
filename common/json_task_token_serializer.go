package common

import "encoding/json"

type (
	jsonTaskTokenSerializer struct{}
)

// NewJSONTaskTokenSerializer creates a new instance of TaskTokenSerializer
func NewJSONTaskTokenSerializer() TaskTokenSerializer {
	return &jsonTaskTokenSerializer{}
}

func (j *jsonTaskTokenSerializer) Serialize(token *TaskToken) ([]byte, error) {
	data, err := json.Marshal(token)

	return data, err
}

func (j *jsonTaskTokenSerializer) Deserialize(data []byte) (*TaskToken, error) {
	var token TaskToken
	err := json.Unmarshal(data, &token)

	return &token, err
}

func (j *jsonTaskTokenSerializer) SerializeQueryTaskToken(token *QueryTaskToken) ([]byte, error) {
	data, err := json.Marshal(token)

	return data, err
}

func (j *jsonTaskTokenSerializer) DeserializeQueryTaskToken(data []byte) (*QueryTaskToken, error) {
	var token QueryTaskToken
	err := json.Unmarshal(data, &token)

	return &token, err
}
