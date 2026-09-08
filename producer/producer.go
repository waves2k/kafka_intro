package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
	"github.com/gofiber/fiber/v3"
)

type KafkaComment struct {
	Text string `json:"text" form:"text"`
}

type ConfigureFiberRoutesFunc func(fiber.Router)

// Abstraction over httpServer.
type Server struct {
	listenAddr string
	app        *fiber.App

	kafkaProducer sarama.SyncProducer
}

// Create a new Server.
func NewServer(listenAddr string, brokersUrls []string) (*Server, error) {
	kafkaProducer, err := connectProducer(brokersUrls)
	if err != nil {
		return nil, fmt.Errorf("error to connect to producer: %w\n", err)
	}

	return &Server{
		listenAddr:    listenAddr,
		app:           fiber.New(),
		kafkaProducer: kafkaProducer,
	}, nil
}

// Init http server routes.
func (s *Server) InitRoutes(routers ...ConfigureFiberRoutesFunc) {
	for index, _ := range routers {
		routers[index](s.app)
	}
}

// Start listening http server.
func (s *Server) Run() error {
	fmt.Printf("Running kafka producer http server on port: [%s]\n", s.listenAddr)

	return s.app.Listen(s.listenAddr)
}

// Shutting down http server.
func (s *Server) Stop() error {
	s.kafkaProducer.Close()

	return s.app.Shutdown()
}

func main() {
	brokerUrl := []string{"localhost:29092"}
	port := ":3000"

	server, err := NewServer(port, brokerUrl)
	if err != nil {
		panic(err)
	}

	server.InitRoutes(
		AddCommentsRoutes(server))

	quitCh := make(chan os.Signal, 2)
	signal.Notify(quitCh, syscall.SIGINT, syscall.SIGTERM)

	server.Run()

	<-quitCh

	server.Stop()
}

func AddCommentsRoutes(s *Server) ConfigureFiberRoutesFunc {
	return func(c fiber.Router) {
		api := c.Group("/api/v1")

		api.Post("/comments", s.handleCreateComment)
	}
}

func (s *Server) handleCreateComment(c fiber.Ctx) error {
	comment := new(KafkaComment)

	if err := c.Bind().Body(comment); err != nil {
		log.Printf("error to parse request body: %v\n", err)

		return c.Status(fiber.StatusBadRequest).JSON(&fiber.Map{
			"success": false,
			"message": err.Error(),
		})
	}

	bytes, err := json.Marshal(comment)
	if err != nil {
		log.Printf("error to marshal comment: %v\n", err)

		return c.Status(fiber.StatusInternalServerError).JSON(&fiber.Map{
			"success": false,
			"message": err.Error(),
		})
	}

	if err := s.PushToQueue("comments", bytes); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(&fiber.Map{
			"success": false,
			"message": err.Error(),
		})
	}

	return c.JSON(&fiber.Map{
		"success": true,
		"message": "comment pushed successfully",
		"comment": comment,
	})

}

func (s *Server) PushToQueue(topic string, message []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	partition, offset, err := s.kafkaProducer.SendMessage(msg)
	if err != nil {
		return err
	}

	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)
	return nil
}

func connectProducer(brokerUrl []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()

	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	return sarama.NewSyncProducer(brokerUrl, config)
}
