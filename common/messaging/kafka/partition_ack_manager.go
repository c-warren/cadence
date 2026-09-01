package kafka

import (
	"errors"
	"sync"

	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/metrics"
)

// Used to convert out of order acks into ackLevel movement,
// assuming reading messages is in order and continuous(no skipping)
type partitionAckManager struct {
	sync.RWMutex
	ackMgrs map[int32]messaging.AckManager // map from partition to its ackManager
	scopes  map[int32]metrics.Scope        // map from partition to its Scope

	metricsClient metrics.Client
	logger        log.Logger
}

func newPartitionAckManager(metricsClient metrics.Client, logger log.Logger) *partitionAckManager {
	return &partitionAckManager{
		ackMgrs: make(map[int32]messaging.AckManager),
		scopes:  make(map[int32]metrics.Scope),

		metricsClient: metricsClient,
		logger:        logger,
	}
}

// AddMessage mark a messageID as read and waiting for completion
func (pam *partitionAckManager) AddMessage(partitionID int32, messageID int64) {
	var err error
	pam.RLock()
	if am, ok := pam.ackMgrs[partitionID]; ok {
		err = am.ReadItem(messageID)
		pam.scopes[partitionID].IncCounter(metrics.KafkaConsumerMessageIn)

		pam.RUnlock()
	} else {
		pam.RUnlock()
		pam.Lock()

		partitionLogger := pam.logger.WithTags(tag.KafkaPartition(partitionID))
		am := messaging.NewContinuousAckManager(partitionLogger)
		pam.ackMgrs[partitionID] = am

		scope := pam.metricsClient.Scope(metrics.MessagingClientConsumerScope, metrics.KafkaPartitionTag(partitionID))
		pam.scopes[partitionID] = scope
		scope.IncCounter(metrics.KafkaConsumerMessageIn)

		err = am.ReadItem(messageID)
		pam.Unlock()
	}
	if err != nil {
		pam.logger.Warn("potential bug when adding message to ackManager", tag.Error(err), tag.KafkaPartition(partitionID), tag.TaskID(messageID))
	}
}

// CompleteMessage complete the message from ack/nack kafka message
func (pam *partitionAckManager) CompleteMessage(partitionID int32, messageID int64, isAck bool) (ackLevel int64, err error) {
	pam.RLock()
	defer pam.RUnlock()
	if am, ok := pam.ackMgrs[partitionID]; ok {
		ackLevel = am.AckItem(messageID)
		if isAck {
			pam.scopes[partitionID].IncCounter(metrics.KafkaConsumerMessageAck)
		} else {
			pam.scopes[partitionID].IncCounter(metrics.KafkaConsumerMessageNack)
		}
	} else {
		return -1, errors.New("Failed to complete an message that hasn't been added to the partition")
	}
	return ackLevel, nil
}
