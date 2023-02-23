package handler

import (
	"kafka-search-consumer-go/pkg/client"
	"kafka-search-consumer-go/pkg/model"
)

type searchHandler struct {
	searchData  *model.SearchData
	earthKafka  client.KafkaConsumer
	secureKafka client.KafkaConsumer
	redis       *client.RedisService
}

func NewSearchHandler(sd *model.SearchData, earthKafka client.KafkaConsumer, secureKafka client.KafkaConsumer, redis *client.RedisService) *searchHandler {
	return &searchHandler{searchData: sd, earthKafka: earthKafka, secureKafka: secureKafka, redis: redis}
}

func (s *searchHandler) Handle() {
	consumer, err := s.hasTopic(s.searchData)
	if err != nil {
		s.redis.SearchException(s.searchData.Id, err)
		return
	}

	ch := NewConsumeHandler(consumer, s.redis)
	ch.handle(s.searchData)
}

func (s *searchHandler) hasTopic(searchData *model.SearchData) (client.KafkaConsumer, error) {
	if _, err := s.earthKafka.GetPartitions(searchData.TopicName); err == nil {
		return s.earthKafka, nil
	}

	_, err := s.secureKafka.GetPartitions(searchData.TopicName)
	if err != nil {
		return nil, err
	}

	return s.secureKafka, nil
}
