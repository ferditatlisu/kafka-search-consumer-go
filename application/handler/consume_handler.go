package handler

import (
	"kafka-search-consumer-go/pkg/client"
	"kafka-search-consumer-go/pkg/model"
	"sync"
	"time"
)

type consumeHandler struct {
	c     client.KafkaConsumer
	redis *client.RedisService
}

func NewConsumeHandler(consumer client.KafkaConsumer, redis *client.RedisService) *consumeHandler {
	return &consumeHandler{c: consumer, redis: redis}
}

func (s *consumeHandler) handle(searchData *model.SearchData) {
	pIds := s.getPartitionIds(searchData)
	if pIds == nil || len(pIds) == 0 {
		return
	}

	var wg sync.WaitGroup
	wg.Add(len(pIds))
	sd := time.Now().UnixMilli()
	s.redis.SearchStarted(searchData.Id)
	for i := range pIds {
		pId := pIds[i]
		go func(sd *model.SearchData, p int, r *client.RedisService) {
			h := NewListenerHandler(sd, s.c, p, r)
			h.Handle()
			wg.Done()
		}(searchData, pId, s.redis)
	}

	wg.Wait()
	s.redis.SearchFinished(searchData.Id)
	sd = time.Now().UnixMilli() - sd
	s.redis.Time(searchData.Id, sd)
}

func (s *consumeHandler) getPartitionIds(searchData *model.SearchData) []int {
	partitions, err := s.c.GetPartitions(searchData.TopicName)
	if err != nil {
		return nil
	}

	pIds := make([]int, len(partitions), len(partitions))
	for i := range partitions {
		pIds[i] = partitions[i].ID
	}

	return pIds
}
