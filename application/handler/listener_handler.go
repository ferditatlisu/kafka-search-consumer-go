package handler

import (
	"context"
	"encoding/json"
	"github.com/segmentio/kafka-go"
	"kafka-search-consumer-go/pkg/client"
	"kafka-search-consumer-go/pkg/model"
	"strings"
	"time"
)

type listenerHandler struct {
	searchData  *model.SearchData
	c           client.KafkaConsumer
	partitionId int
	r           *client.RedisService
}

func NewListenerHandler(sd *model.SearchData, consumer client.KafkaConsumer, partitionId int, r *client.RedisService) *listenerHandler {
	return &listenerHandler{searchData: sd, c: consumer, partitionId: partitionId, r: r}
}

func (l *listenerHandler) Handle() {
	sd := time.Now().UTC().UnixMilli()
	kc := l.c.CreateConsumer(l.searchData.TopicName, l.partitionId)
	defer kc.Close()
	ctx, cancel := context.WithCancel(context.Background())
	t := time.AfterFunc(time.Second*10, func() {
		cancel()
	})

	for {
		m, err := kc.ReadMessage(ctx)
		if err != nil || m.Time.UnixMilli() > sd {
			break
		}

		t.Reset(time.Second * 5)
		if !(l.searchData.Key == nil || *l.searchData.Key == "") {
			kStr := string(m.Key[:])
			has := strings.Contains(kStr, *l.searchData.Key)
			if has {
				l.founded(&m)
				continue
			}
		}

		if !(l.searchData.Value == nil || *l.searchData.Value == "") {
			vStr := string(m.Value[:])
			has := strings.Contains(vStr, *l.searchData.Value)
			if has {
				l.founded(&m)
				continue
			}
		}
	}
}

func (l *listenerHandler) founded(m *kafka.Message) {
	sec := map[string]interface{}{}
	if err := json.Unmarshal(m.Value, &sec); err != nil {
		l.r.SearchException(l.searchData.Id, err)
		return
	}
	sec["__kafka_offset"] = m.Offset
	sec["__kafka_partition"] = m.Partition
	sec["__kafka_publish_date_utc"] = m.Time.UnixMilli()
	value, err := json.Marshal(sec)
	if err != nil {
		l.r.SearchException(l.searchData.Id, err)
		return
	}
	l.r.Save(l.searchData.Id, string(value))
}
