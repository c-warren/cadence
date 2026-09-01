package kafka

import (
	"context"
	"errors"

	"github.com/IBM/sarama"

	"github.com/uber/cadence/.gen/go/indexer"
	"github.com/uber/cadence/.gen/go/sqlblobs"
	"github.com/uber/cadence/common/codec"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/messaging"
)

type (
	producerImpl struct {
		topic      string
		producer   sarama.SyncProducer
		msgEncoder codec.BinaryEncoder
		logger     log.Logger
	}
)

var _ messaging.Producer = (*producerImpl)(nil)

// NewKafkaProducer is used to create the Kafka based producer implementation
func NewKafkaProducer(topic string, producer sarama.SyncProducer, logger log.Logger) messaging.Producer {
	return &producerImpl{
		topic:      topic,
		producer:   producer,
		msgEncoder: codec.NewThriftRWEncoder(),
		logger:     logger.WithTags(tag.KafkaTopicName(topic)),
	}
}

// Publish is used to send messages to other clusters through Kafka topic
// TODO implement context when https://github.com/IBM/sarama/issues/1849 is supported
func (p *producerImpl) Publish(_ context.Context, msg interface{}) error {
	message, err := p.getProducerMessage(msg)
	if err != nil {
		return err
	}

	partition, offset, err := p.producer.SendMessage(message)
	if err != nil {
		p.logger.Warn("Failed to publish message to kafka",
			tag.KafkaPartition(partition),
			tag.KafkaPartitionKey(message.Key),
			tag.KafkaOffset(offset),
			tag.Error(err))
		return p.convertErr(err)
	}

	return nil
}

// Close is used to close Kafka publisher
func (p *producerImpl) Close() error {
	return p.convertErr(p.producer.Close())
}

func (p *producerImpl) serializeThrift(input codec.ThriftObject) ([]byte, error) {
	payload, err := p.msgEncoder.Encode(input)
	if err != nil {
		p.logger.Error("Failed to serialize thrift object", tag.Error(err))

		return nil, err
	}

	return payload, nil
}

func (p *producerImpl) getProducerMessage(message interface{}) (*sarama.ProducerMessage, error) {
	switch message := message.(type) {
	case *indexer.Message:
		payload, err := p.serializeThrift(message)
		if err != nil {
			return nil, err
		}
		msg := &sarama.ProducerMessage{
			Topic: p.topic,
			Key:   sarama.StringEncoder(message.GetWorkflowID()),
			Value: sarama.ByteEncoder(payload),
		}
		return msg, nil
	case *sarama.ConsumerMessage:
		msg := &sarama.ProducerMessage{
			Topic: p.topic,
			Key:   sarama.ByteEncoder(message.Key),
			Value: sarama.ByteEncoder(message.Value),
		}
		return msg, nil
	case *indexer.PinotMessage:
		msg := &sarama.ProducerMessage{
			Topic: p.topic,
			Key:   sarama.StringEncoder(message.GetWorkflowID()),
			Value: sarama.ByteEncoder(message.GetPayload()),
		}
		return msg, nil
	case *sqlblobs.AsyncRequestMessage:
		payload, err := p.serializeThrift(message)
		if err != nil {
			return nil, err
		}
		msg := &sarama.ProducerMessage{
			Topic: p.topic,
			Key:   sarama.StringEncoder(message.GetPartitionKey()),
			Value: sarama.ByteEncoder(payload),
		}
		return msg, nil
	default:
		return nil, errors.New("unknown producer message type")
	}
}

func (p *producerImpl) convertErr(err error) error {
	switch err {
	case sarama.ErrMessageSizeTooLarge:
		return messaging.ErrMessageSizeLimit
	default:
		return err
	}
}
