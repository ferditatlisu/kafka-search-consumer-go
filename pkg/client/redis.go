package client

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"kafka-search-consumer-go/pkg/config"
	"time"
)

type RedisService struct {
	client *redis.Client
	ctx    context.Context
}

func NewRedisService(rc config.RedisConfig) *RedisService {
	rdb := redis.NewClient(&redis.Options{
		Addr:     rc.Host,
		Password: rc.Password, // no password set
		DB:       rc.Database, // use default DB
	})

	return &RedisService{rdb, context.Background()}
}

func (r *RedisService) Save(key string, value string) {
	_ = r.client.RPush(r.ctx, key, value).Err()
	r.searchExpire(key)
}

func (r *RedisService) SearchStarted(key string) {
	v := fmt.Sprintf("{\"%s\":\"%s\"}", "state", "started")
	_ = r.client.RPush(r.ctx, key, v).Err()
	r.searchExpire(key)
}

func (r *RedisService) SearchValidationError(key string) {
	v := fmt.Sprintf("{\"%s\":\"%s\"}", "state", "parameters are not valid.")
	_ = r.client.RPush(r.ctx, key, v).Err()
	r.searchExpire(key)
}

func (r *RedisService) Time(key string, dt int64) {
	_ = r.client.Set(r.ctx, "go-time of "+key, dt, time.Hour*3).Err()
}

func (r *RedisService) SearchFinished(key string) {
	v := fmt.Sprintf("{\"%s\":\"%s\"}", "state", "finished")
	_ = r.client.RPush(r.ctx, key, v).Err()
	r.searchExpire(key)
}

func (r *RedisService) SearchException(key string, err error) {
	v := fmt.Sprintf("{\"%s\":\"%s\"}", "state", err.Error())
	_ = r.client.RPush(r.ctx, key, v).Err()
	r.searchExpire(key)
}

func (r *RedisService) searchExpire(key string) {
	_ = r.client.Expire(r.ctx, key, time.Minute*10).Err()
}
