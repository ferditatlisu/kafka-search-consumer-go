package client

import (
	"context"
	"errors"
	"github.com/segmentio/kafka-go"
	"strings"
)

type KafkaConsumer interface {
	ReadMessage(consumer *kafka.Reader) (kafka.Message, error)
	Stop(consumer *kafka.Reader)
	GetPartitions(topicName string) ([]kafka.Partition, error)
	CreateConsumer(topic string, partition int) *kafka.Reader
}

type earthKafkaConsumer struct {
	s string
}

func NewEarthKafkaConsumer(servers string) KafkaConsumer {
	return &earthKafkaConsumer{servers}
}

func (k *earthKafkaConsumer) CreateConsumer(topic string, partition int) *kafka.Reader {
	readerConfig := kafka.ReaderConfig{
		Brokers:   strings.Split(k.s, ","),
		Partition: partition,
		Topic:     topic,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	}

	newConsumer := kafka.NewReader(readerConfig)
	return newConsumer
}

func (k *earthKafkaConsumer) ReadMessage(consumer *kafka.Reader) (kafka.Message, error) {
	msg, err := consumer.ReadMessage(context.Background())
	return msg, err
}

func (k *earthKafkaConsumer) Stop(consumer *kafka.Reader) {
	_ = consumer.Close()
}

func (k *earthKafkaConsumer) GetPartitions(topicName string) ([]kafka.Partition, error) {
	server := strings.Split(k.s, ",")[0]
	dial, err := kafka.Dial("tcp", server)
	if err == nil {
		partitions, err := dial.ReadPartitions(topicName)
		return partitions, err
	}

	return nil, errors.New("Can not connect kafka host")
}
