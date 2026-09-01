package types

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

func Test_Error(t *testing.T) {
	errMessage := "test"
	tests := []struct {
		name string
		err  error
	}{
		{
			err: AccessDeniedError{
				Message: errMessage,
			},
		},
		{
			err: BadRequestError{
				Message: errMessage,
			},
		},
		{
			err: CancellationAlreadyRequestedError{
				Message: errMessage,
			},
		},
		{
			err: DomainAlreadyExistsError{
				Message: errMessage,
			},
		},
		{
			err: EntityNotExistsError{
				Message: errMessage,
			},
		},
		{
			err: InternalDataInconsistencyError{
				Message: errMessage,
			},
		},
		{
			err: WorkflowExecutionAlreadyCompletedError{
				Message: errMessage,
			},
		},
		{
			err: LimitExceededError{
				Message: errMessage,
			},
		},
		{
			err: QueryFailedError{
				Message: errMessage,
			},
		},
		{
			err: RemoteSyncMatchedError{
				Message: errMessage,
			},
		},
		{
			err: ServiceBusyError{
				Message: errMessage,
			},
		},
		{
			err: EventAlreadyStartedError{
				Message: errMessage,
			},
		},
		{
			err: StickyWorkerUnavailableError{
				Message: errMessage,
			},
		},
		{
			err: ReadOnlyPartitionError{
				Message: errMessage,
			},
		},
		{
			err: InternalServiceError{
				Message: errMessage,
			},
		},
	}
	for _, tt := range tests {
		t.Run(reflect.TypeOf(tt.err).String(), func(t *testing.T) {
			require.Equal(t, errMessage, tt.err.Error())
		})
	}
}

func Test_ClientVersionNotSupportedError(t *testing.T) {
	err := ClientVersionNotSupportedError{
		FeatureVersion:    "1.0",
		ClientImpl:        "1.0",
		SupportedVersions: "1.2",
	}
	require.Equal(t, "client version not supported", err.Error())
	require.NoError(t, err.MarshalLogObject(zapcore.NewMapObjectEncoder()))
}

func Test_FeatureNotEnabledError(t *testing.T) {
	err := FeatureNotEnabledError{FeatureFlag: "test"}
	require.Equal(t, "feature not enabled", err.Error())
	require.NoError(t, err.MarshalLogObject(zapcore.NewMapObjectEncoder()))
}

func Test_CurrentBranchChangedError(t *testing.T) {
	err := CurrentBranchChangedError{Message: "test", CurrentBranchToken: []byte{}}
	require.Equal(t, "test", err.Error())
	require.NoError(t, err.MarshalLogObject(zapcore.NewMapObjectEncoder()))
}

func Test_DomainNotActiveError(t *testing.T) {
	err := DomainNotActiveError{Message: "test", DomainName: "test-domain"}
	require.Equal(t, "test", err.Error())
	require.NoError(t, err.MarshalLogObject(zapcore.NewMapObjectEncoder()))
}

func Test_RetryTaskV2Error(t *testing.T) {
	testID := int64(1)
	testVersion := int64(1.0)
	err := RetryTaskV2Error{
		Message:           "test",
		DomainID:          "test-domain-id",
		WorkflowID:        "wid",
		RunID:             "rid",
		StartEventID:      &testID,
		StartEventVersion: &testVersion,
		EndEventID:        &testID,
		EndEventVersion:   &testVersion,
	}
	require.Equal(t, "test", err.Error())
	require.NoError(t, err.MarshalLogObject(zapcore.NewMapObjectEncoder()))
}

func Test_WorkflowExecutionAlreadyStartedError(t *testing.T) {
	err := WorkflowExecutionAlreadyStartedError{Message: "test"}
	require.Equal(t, "test", err.Error())
	require.NoError(t, err.MarshalLogObject(zapcore.NewMapObjectEncoder()))
}

func Test_ShardOwnershipLostError(t *testing.T) {
	err := ShardOwnershipLostError{Message: "test"}
	require.Equal(t, "test", err.Error())
	require.NoError(t, err.MarshalLogObject(zapcore.NewMapObjectEncoder()))
}
