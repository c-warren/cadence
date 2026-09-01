package config

import "fmt"

type (
	// KafkaConfig describes the configuration needed to connect to all kafka clusters
	KafkaConfig struct {
		TLS      TLS                      `yaml:"tls"`
		SASL     SASL                     `yaml:"sasl"`
		Clusters map[string]ClusterConfig `yaml:"clusters"`
		Topics   map[string]TopicConfig   `yaml:"topics"`
		// Applications describes the applications that will use the Kafka topics
		Applications map[string]TopicList `yaml:"applications"`
		Version      string               `yaml:"version"`
	}

	// ClusterConfig describes the configuration for a single Kafka cluster
	ClusterConfig struct {
		Brokers []string `yaml:"brokers"`
	}

	// TopicConfig describes the mapping from topic to Kafka cluster
	TopicConfig struct {
		Cluster string `yaml:"cluster"`
		// Properties map describes whether the topic properties, such as whether it is secure
		Properties map[string]any `yaml:"properties,omitempty"`
	}

	// TopicList describes the topic names for each cluster
	TopicList struct {
		Topic    string `yaml:"topic"`
		DLQTopic string `yaml:"dlq-topic"`
	}
)

// Validate will validate config for kafka
func (k *KafkaConfig) Validate(checkApp bool) {
	if len(k.Clusters) == 0 {
		panic("Empty Kafka Cluster Config")
	}
	if len(k.Topics) == 0 {
		panic("Empty Topics Config")
	}

	validateTopicsFn := func(topic string) {
		if topic == "" {
			panic("Empty Topic Name")
		} else if topicConfig, ok := k.Topics[topic]; !ok {
			panic(fmt.Sprintf("Missing Topic Config for Topic %v", topic))
		} else if clusterConfig, ok := k.Clusters[topicConfig.Cluster]; !ok {
			panic(fmt.Sprintf("Missing Kafka Cluster Config for Cluster %v", topicConfig.Cluster))
		} else if len(clusterConfig.Brokers) == 0 {
			panic(fmt.Sprintf("Missing Kafka Brokers Config for Cluster %v", topicConfig.Cluster))
		}
	}

	if checkApp {
		if len(k.Applications) == 0 {
			panic("Empty Applications Config")
		}
		for _, topics := range k.Applications {
			validateTopicsFn(topics.Topic)
			validateTopicsFn(topics.DLQTopic)
		}
	}
}

// GetKafkaClusterForTopic gets cluster from topic
func (k *KafkaConfig) GetKafkaClusterForTopic(topic string) string {
	return k.Topics[topic].Cluster
}

// GetBrokersForKafkaCluster gets broker from cluster
func (k *KafkaConfig) GetBrokersForKafkaCluster(kafkaCluster string) []string {
	return k.Clusters[kafkaCluster].Brokers
}

// GetTopicsForApplication gets topic from application
func (k *KafkaConfig) GetTopicsForApplication(app string) TopicList {
	return k.Applications[app]
}

// GetKafkaPropertiesForTopic gets properties from topic
func (k *KafkaConfig) GetKafkaPropertyForTopic(topic string, property string) any {
	topicConfig, ok := k.Topics[topic]
	if !ok || topicConfig.Properties == nil {
		// No properties for the specified topic in the config
		return nil
	}

	// retrieve the property from the topic properties
	propertyValue, ok := topicConfig.Properties[property]
	if !ok {
		// Property not found
		return nil
	}

	return propertyValue
}
