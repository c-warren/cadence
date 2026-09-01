package consumer

import "github.com/uber/cadence/.gen/go/sqlblobs"

type UnsupportedRequestType struct {
	Type sqlblobs.AsyncRequestType
}

func (e *UnsupportedRequestType) Error() string {
	return "unsupported request type: " + e.Type.String()
}

type UnsupportedEncoding struct {
	EncodingType string
}

func (e *UnsupportedEncoding) Error() string {
	return "unsupported encoding: " + e.EncodingType
}
