package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gin-contrib/gzip"
	"github.com/gin-gonic/gin"
	"kafka-search-consumer-go/application/handler"
	"kafka-search-consumer-go/pkg/client"
	"kafka-search-consumer-go/pkg/config"
	"kafka-search-consumer-go/pkg/logger"
	"kafka-search-consumer-go/pkg/model"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	gracefulShutdown := createGracefulShutdownChannel()
	go func() {
		router := setupRouter()
		_ = router.Run(":8082")
	}()
	go func() {
		configInstance := config.CreateConfigInstance()
		applicationConfig, _ := configInstance.GetConfig()
		earthKafka := client.NewEarthKafkaConsumer(applicationConfig.EarthKafka.Servers)
		secureKafka := client.NewSecureKafkaConsumer(&applicationConfig.SecureKafka)
		redis := client.NewRedisService(applicationConfig.Redis)
		skc := client.NewSearchKafkaConsumer(applicationConfig.SearchKafka)
		ctx := context.Background()
		for {
			msg, err := skc.ReadMessage(ctx)
			if err != nil {
				skc.Stop()
				break
			}

			var data *model.SearchData
			err = json.Unmarshal(msg.Value, &data)
			if err != nil {
				fmt.Printf("Unmarshal error occured :  %s\n", string(msg.Value))
				return
			}

			fmt.Printf("Message consumed %s\n", data.Id)
			if data.TopicName == "" ||
				((data.Key == nil || *data.Key == "") && (data.Value == nil || *data.Value == "")) {
				redis.SearchValidationError(data.Id)
				continue
			}

			sh := handler.NewSearchHandler(data, earthKafka, secureKafka, redis)
			sh.Handle()
		}
	}()
	<-gracefulShutdown
	_, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer func() {
		logger.Logger().Info("gracefully shutting down...")
		_ = logger.Logger().Sync()
		cancel()
	}()
}

func createGracefulShutdownChannel() chan os.Signal {
	gracefulShutdown := make(chan os.Signal, 1)
	signal.Notify(gracefulShutdown, syscall.SIGTERM)
	signal.Notify(gracefulShutdown, syscall.SIGINT)
	return gracefulShutdown
}

func setupRouter() *gin.Engine {
	router := gin.New()
	router.Use(gzip.Gzip(gzip.BestCompression))
	router.Use(gin.Recovery())
	//health Check
	router.GET("/_monitoring/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "UP"})
	})
	router.GET("", func(c *gin.Context) {
		c.Redirect(302, "/swagger/index.html")
		c.Abort()
	})
	return router
}
