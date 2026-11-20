package persistencetests

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/uber/cadence/common/constants"
	"github.com/uber/cadence/common/persistence"
)

// AsyncWorkflowQueuePersistenceSuite exercises AsyncWorkflowQueueManager against a live store.
type AsyncWorkflowQueuePersistenceSuite struct {
	*TestBase
	*require.Assertions
}

func (s *AsyncWorkflowQueuePersistenceSuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}
}

func (s *AsyncWorkflowQueuePersistenceSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *AsyncWorkflowQueuePersistenceSuite) TearDownSuite() {
	s.TearDownWorkflowStore()
}

func (s *AsyncWorkflowQueuePersistenceSuite) TestAsyncWorkflowQueuePersistence() {
	ctx, cancel := context.WithTimeout(context.Background(), testContextTimeout)
	defer cancel()

	manager, err := s.PersistenceFactory.NewAsyncWorkflowQueueManager()
	s.NoError(err)
	s.NotNil(manager)
	defer manager.Close()

	scenarios := []struct {
		name string
		run  func(context.Context, persistence.AsyncWorkflowQueueManager, int, string)
	}{
		{name: "enqueue and read paging", run: s.testEnqueueAndReadPaging},
		{name: "duplicate message id is rejected", run: s.testDuplicateMessageRejected},
		{name: "ack level monotonicity", run: s.testAckLevelMonotonicity},
		{name: "range delete", run: s.testRangeDelete},
	}

	for index, scenario := range scenarios {
		scenario := scenario
		shardID := 7000 + index
		sourceCluster := scenario.name + "-" + uuid.NewString()
		s.Run(scenario.name, func() {
			scenario.run(ctx, manager, shardID, sourceCluster)
		})
	}
}

func (s *AsyncWorkflowQueuePersistenceSuite) testEnqueueAndReadPaging(
	ctx context.Context,
	manager persistence.AsyncWorkflowQueueManager,
	shardID int,
	sourceCluster string,
) {
	for messageID := int64(10); messageID <= 16; messageID++ {
		s.NoError(manager.EnqueueAsyncWorkflowMessage(ctx, asyncWorkflowEnqueueRequest(shardID, sourceCluster, messageID, fmt.Sprintf("workflow-%d", messageID))))
	}

	var messages []*persistence.AsyncWorkflowMessage
	after := int64(0)
	for {
		response, err := manager.ReadAsyncWorkflowMessages(ctx, &persistence.ReadAsyncWorkflowMessagesRequest{
			ShardID: shardID, SourceCluster: sourceCluster, ExclusiveMinMessageID: after, PageSize: 3,
		})
		s.NoError(err)
		if len(response.Messages) == 0 {
			break
		}
		messages = append(messages, response.Messages...)
		after = response.Messages[len(response.Messages)-1].MessageID
	}

	s.Len(messages, 7)
	for index, message := range messages {
		messageID := int64(10 + index)
		s.Equal(messageID, message.MessageID)
		s.Equal(shardID, message.ShardID)
		s.Equal(sourceCluster, message.SourceCluster)
		s.Equal("domain", message.DomainName)
		s.Equal(fmt.Sprintf("workflow-%d", messageID), message.WorkflowID)
		s.Equal(fmt.Sprintf("request-%d", messageID), message.RequestID)
		s.Equal(persistence.AsyncWorkflowRequestType(messageID%2), message.RequestType)
		s.Equal([]byte{byte(messageID), 1, 2}, message.Payload)
		s.Equal(constants.EncodingTypeThriftRW, message.PayloadEncoding)
		s.False(message.CreatedTime.IsZero())
	}

	otherSource, err := manager.ReadAsyncWorkflowMessages(ctx, &persistence.ReadAsyncWorkflowMessagesRequest{
		ShardID: shardID, SourceCluster: "other-" + uuid.NewString(), PageSize: 3,
	})
	s.NoError(err)
	s.Empty(otherSource.Messages)
	otherShard, err := manager.ReadAsyncWorkflowMessages(ctx, &persistence.ReadAsyncWorkflowMessagesRequest{
		ShardID: shardID + 1000, SourceCluster: sourceCluster, PageSize: 3,
	})
	s.NoError(err)
	s.Empty(otherShard.Messages)
}

func (s *AsyncWorkflowQueuePersistenceSuite) testDuplicateMessageRejected(
	ctx context.Context,
	manager persistence.AsyncWorkflowQueueManager,
	shardID int,
	sourceCluster string,
) {
	s.NoError(manager.EnqueueAsyncWorkflowMessage(ctx, asyncWorkflowEnqueueRequest(shardID, sourceCluster, 10, "first")))

	err := manager.EnqueueAsyncWorkflowMessage(ctx, asyncWorkflowEnqueueRequest(shardID, sourceCluster, 10, "second"))
	var conditionFailed *persistence.ConditionFailedError
	s.ErrorAs(err, &conditionFailed)

	response, err := manager.ReadAsyncWorkflowMessages(ctx, &persistence.ReadAsyncWorkflowMessagesRequest{
		ShardID: shardID, SourceCluster: sourceCluster, PageSize: 10,
	})
	s.NoError(err)
	s.Len(response.Messages, 1)
	s.Equal("first", response.Messages[0].WorkflowID)
}

func (s *AsyncWorkflowQueuePersistenceSuite) testAckLevelMonotonicity(
	ctx context.Context,
	manager persistence.AsyncWorkflowQueueManager,
	shardID int,
	sourceCluster string,
) {
	levels, err := manager.GetAsyncWorkflowAckLevels(ctx, &persistence.GetAsyncWorkflowAckLevelsRequest{ShardID: shardID})
	s.NoError(err)
	s.Empty(levels.AckLevels)

	for _, step := range []struct {
		input int64
		want  int64
	}{
		{input: 0, want: 0},
		{input: 5, want: 5},
		{input: 3, want: 5},
		{input: 5, want: 5},
		{input: 9, want: 9},
	} {
		s.NoError(manager.UpdateAsyncWorkflowAckLevel(ctx, &persistence.UpdateAsyncWorkflowAckLevelRequest{
			ShardID: shardID, SourceCluster: sourceCluster, AckLevel: step.input,
		}))
		levels, err = manager.GetAsyncWorkflowAckLevels(ctx, &persistence.GetAsyncWorkflowAckLevelsRequest{ShardID: shardID})
		s.NoError(err)
		s.Contains(levels.AckLevels, sourceCluster)
		s.Equal(step.want, levels.AckLevels[sourceCluster])
	}

	secondSource := "second-" + uuid.NewString()
	s.NoError(manager.UpdateAsyncWorkflowAckLevel(ctx, &persistence.UpdateAsyncWorkflowAckLevelRequest{
		ShardID: shardID, SourceCluster: secondSource, AckLevel: 4,
	}))
	levels, err = manager.GetAsyncWorkflowAckLevels(ctx, &persistence.GetAsyncWorkflowAckLevelsRequest{ShardID: shardID})
	s.NoError(err)
	s.Equal(map[string]int64{sourceCluster: 9, secondSource: 4}, levels.AckLevels)

	otherShard, err := manager.GetAsyncWorkflowAckLevels(ctx, &persistence.GetAsyncWorkflowAckLevelsRequest{ShardID: shardID + 1000})
	s.NoError(err)
	s.Empty(otherShard.AckLevels)

	// Control rows never show up in message reads.
	s.NoError(manager.EnqueueAsyncWorkflowMessage(ctx, asyncWorkflowEnqueueRequest(shardID, sourceCluster, 10, "workflow")))
	messages, err := manager.ReadAsyncWorkflowMessages(ctx, &persistence.ReadAsyncWorkflowMessagesRequest{
		ShardID: shardID, SourceCluster: sourceCluster, PageSize: 10,
	})
	s.NoError(err)
	s.Len(messages.Messages, 1)
	s.Equal(int64(10), messages.Messages[0].MessageID)
}

func (s *AsyncWorkflowQueuePersistenceSuite) testRangeDelete(
	ctx context.Context,
	manager persistence.AsyncWorkflowQueueManager,
	shardID int,
	sourceCluster string,
) {
	secondSource := "second-" + uuid.NewString()
	for messageID := int64(1); messageID <= 5; messageID++ {
		s.NoError(manager.EnqueueAsyncWorkflowMessage(ctx, asyncWorkflowEnqueueRequest(shardID, sourceCluster, messageID, fmt.Sprintf("workflow-%d", messageID))))
	}
	for messageID := int64(1); messageID <= 2; messageID++ {
		s.NoError(manager.EnqueueAsyncWorkflowMessage(ctx, asyncWorkflowEnqueueRequest(shardID, secondSource, messageID, fmt.Sprintf("other-%d", messageID))))
	}
	s.NoError(manager.UpdateAsyncWorkflowAckLevel(ctx, &persistence.UpdateAsyncWorkflowAckLevelRequest{
		ShardID: shardID, SourceCluster: sourceCluster, AckLevel: 3,
	}))
	s.NoError(manager.RangeDeleteAsyncWorkflowMessages(ctx, &persistence.RangeDeleteAsyncWorkflowMessagesRequest{
		ShardID: shardID, SourceCluster: sourceCluster, InclusiveMaxMessageID: 3,
	}))

	remaining, err := manager.ReadAsyncWorkflowMessages(ctx, &persistence.ReadAsyncWorkflowMessagesRequest{
		ShardID: shardID, SourceCluster: sourceCluster, PageSize: 10,
	})
	s.NoError(err)
	s.Equal([]int64{4, 5}, asyncWorkflowMessageIDs(remaining.Messages))
	// The other source cluster's rows and the control row survive the range delete.
	other, err := manager.ReadAsyncWorkflowMessages(ctx, &persistence.ReadAsyncWorkflowMessagesRequest{
		ShardID: shardID, SourceCluster: secondSource, PageSize: 10,
	})
	s.NoError(err)
	s.Equal([]int64{1, 2}, asyncWorkflowMessageIDs(other.Messages))
	levels, err := manager.GetAsyncWorkflowAckLevels(ctx, &persistence.GetAsyncWorkflowAckLevelsRequest{ShardID: shardID})
	s.NoError(err)
	s.Equal(int64(3), levels.AckLevels[sourceCluster])
}

func asyncWorkflowEnqueueRequest(shardID int, sourceCluster string, messageID int64, workflowID string) *persistence.EnqueueAsyncWorkflowMessageRequest {
	return &persistence.EnqueueAsyncWorkflowMessageRequest{
		ShardID:         shardID,
		SourceCluster:   sourceCluster,
		MessageID:       messageID,
		DomainName:      "domain",
		WorkflowID:      workflowID,
		RequestID:       fmt.Sprintf("request-%d", messageID),
		RequestType:     persistence.AsyncWorkflowRequestType(messageID % 2),
		Payload:         []byte{byte(messageID), 1, 2},
		PayloadEncoding: constants.EncodingTypeThriftRW,
	}
}

func asyncWorkflowMessageIDs(messages []*persistence.AsyncWorkflowMessage) []int64 {
	ids := make([]int64, 0, len(messages))
	for _, message := range messages {
		ids = append(ids, message.MessageID)
	}
	return ids
}
