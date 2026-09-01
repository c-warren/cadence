package elasticsearch

import (
	"net/http"
	"time"

	"github.com/uber/cadence/common/config"
)

const (
	// TODO https://github.com/uber/cadence/issues/3686
	oneMicroSecondInNano = int64(time.Microsecond / time.Nanosecond)

	esDocIDDelimiter = "~"
	esDocType        = "_doc"
	esDocIDSizeLimit = 512
)

// Build Http Client with TLS
func buildTLSHTTPClient(config config.TLS) (*http.Client, error) {
	tlsConfig, err := config.ToTLSConfig()
	if err != nil {
		return nil, err
	}

	// Setup HTTPS client
	transport := &http.Transport{TLSClientConfig: tlsConfig}
	tlsClient := &http.Client{Transport: transport}

	return tlsClient, nil
}

func GetESDocIDSizeLimit() int {
	return esDocIDSizeLimit
}

func GetESDocType() string {
	return esDocType
}

func GetESDocDelimiter() string {
	return esDocIDDelimiter
}

func GenerateDocID(wid, rid string) string {
	return wid + esDocIDDelimiter + rid
}
