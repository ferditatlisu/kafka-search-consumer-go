package client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
	"kafka-search-consumer-go/pkg/config"
	"os"
	"strings"
)

type secureKafkaConsumer struct {
	c *config.SecureKafkaConfig
}

func NewSecureKafkaConsumer(c *config.SecureKafkaConfig) KafkaConsumer {
	return &secureKafkaConsumer{c}
}

func (k *secureKafkaConsumer) CreateConsumer(topic string, partition int) *kafka.Reader {
	readerConfig := kafka.ReaderConfig{
		Brokers:   strings.Split(k.c.Servers, ","),
		Partition: partition,
		Topic:     topic,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
		Dialer:    createKafkaDialer(k.c, createTLSConfig(k.c)),
	}

	newConsumer := kafka.NewReader(readerConfig)
	return newConsumer
}

func (k *secureKafkaConsumer) ReadMessage(consumer *kafka.Reader) (kafka.Message, error) {
	msg, err := consumer.ReadMessage(context.Background())
	return msg, err
}

func (k *secureKafkaConsumer) Stop(consumer *kafka.Reader) {
	_ = consumer.Close()
}

func (k *secureKafkaConsumer) GetPartitions(topicName string) ([]kafka.Partition, error) {
	server := strings.Split(k.c.Servers, ",")[0]
	dialer := createKafkaDialer(k.c, createTLSConfig(k.c))
	partitions, err := dialer.LookupPartitions(context.Background(), "tcp", server, topicName)
	return partitions, err
}

func createKafkaDialer(kafkaConfig *config.SecureKafkaConfig, tlsConfig *tls.Config) *kafka.Dialer {
	mechanism, err := scram.Mechanism(scram.SHA512, kafkaConfig.UserName, kafkaConfig.Password)
	if err != nil {
		panic("Error while creating SCRAM configuration, error: " + err.Error())
	}

	return &kafka.Dialer{
		TLS:           tlsConfig,
		SASLMechanism: mechanism,
	}
}

func createTLSConfig(kafkaConfig *config.SecureKafkaConfig) *tls.Config {
	rootCA, err := os.ReadFile(kafkaConfig.RootCAPath)
	if err != nil {
		panic("Error while reading Root CA file: " + kafkaConfig.RootCAPath + " error: " + err.Error())
	}

	interCA, err := os.ReadFile(kafkaConfig.IntermediateCAPath)
	if err != nil {
		panic("Error while reading Intermediate CA file: " + kafkaConfig.IntermediateCAPath + " error: " + err.Error())
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(rootCA)
	caCertPool.AppendCertsFromPEM(interCA)

	return &tls.Config{
		RootCAs: caCertPool,
	}
}
