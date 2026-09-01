package filestore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/dgryski/go-farm"

	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/common/util"
)

var (
	errEmptyDirectoryPath = errors.New("directory path is empty")
)

// encoding & decoding util

func encode(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func decodeHistoryBatches(data []byte) ([]*types.History, error) {
	historyBatches := []*types.History{}
	err := json.Unmarshal(data, &historyBatches)
	if err != nil {
		return nil, err
	}
	return historyBatches, nil
}

func decodeVisibilityRecord(data []byte) (*visibilityRecord, error) {
	record := &visibilityRecord{}
	err := json.Unmarshal(data, record)
	if err != nil {
		return nil, err
	}
	return record, nil
}

func serializeToken(token interface{}) ([]byte, error) {
	if token == nil {
		return nil, nil
	}
	return json.Marshal(token)
}

func deserializeGetHistoryToken(bytes []byte) (*getHistoryToken, error) {
	token := &getHistoryToken{}
	err := json.Unmarshal(bytes, token)
	return token, err
}

func deserializeQueryVisibilityToken(bytes []byte) (*queryVisibilityToken, error) {
	token := &queryVisibilityToken{}
	err := json.Unmarshal(bytes, token)
	return token, err
}

// File name construction

func constructHistoryFilename(domainID, workflowID, runID string, version int64) string {
	combinedHash := constructHistoryFilenamePrefix(domainID, workflowID, runID)
	return fmt.Sprintf("%s_%v.history", combinedHash, version)
}

func constructHistoryFilenamePrefix(domainID, workflowID, runID string) string {
	return strings.Join([]string{hash(domainID), hash(workflowID), hash(runID)}, "")
}

func constructVisibilityFilename(closeTimestamp int64, runID string) string {
	return fmt.Sprintf("%v_%s.visibility", closeTimestamp, hash(runID))
}

func hash(s string) string {
	return fmt.Sprintf("%v", farm.Fingerprint64([]byte(s)))
}

// Validation

func validateDirPath(dirPath string) error {
	if len(dirPath) == 0 {
		return errEmptyDirectoryPath
	}
	info, err := os.Stat(dirPath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return util.ErrDirectoryExpected
	}
	return nil
}

// Misc.

func extractCloseFailoverVersion(filename string) (int64, error) {
	filenameParts := strings.FieldsFunc(filename, func(r rune) bool {
		return r == '_' || r == '.'
	})
	if len(filenameParts) != 3 {
		return -1, errors.New("unknown filename structure")
	}
	return strconv.ParseInt(filenameParts[1], 10, 64)
}

func contextExpired(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}
