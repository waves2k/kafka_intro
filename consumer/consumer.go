package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
)

type Consumer struct {
	messageCount int64

	consumer   sarama.Consumer
	partitions []sarama.PartitionConsumer

	quitCh chan interface{}
}

func connectConsumer(brokersUrls []string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	return sarama.NewConsumer(brokersUrls, config)
}

// Create new Kafka Consumer.
func NewConsumer(brokersUrls []string) (*Consumer, error) {
	consumer, err := connectConsumer(brokersUrls)
	if err != nil {
		return nil, fmt.Errorf("error to create new consumer: %w\n", err)
	}

	return &Consumer{
		consumer:   consumer,
		partitions: make([]sarama.PartitionConsumer, 0),
		quitCh:     make(chan interface{}),
	}, nil
}

func (c *Consumer) AddTopic(topic string, partition int32, offset int64) error {
	pConsumer, err := c.consumer.ConsumePartition(topic, partition, offset)
	if err != nil {
		return fmt.Errorf("error to consume partition: %w\n", err)
	}

	c.partitions = append(c.partitions, pConsumer)
	return nil
}

// Start listenning all the topics.
func (c *Consumer) Run() {
	for index, _ := range c.partitions {
		go c.listenPartitionConsumer(c.partitions[index])
	}
}

// Gracefull shutdown.
func (c *Consumer) Stop() {
	close(c.quitCh)
}

func (c *Consumer) listenPartitionConsumer(p sarama.PartitionConsumer) {
	for {
		select {
		case <-c.quitCh:
			fmt.Println("Interruption detected")
			return
		case err := <-p.Errors():
			fmt.Println(err)
		case message := <-p.Messages():
			c.messageCount++
			fmt.Printf("Recieved message Count: %d | Topic: %s | Message: %s\n", c.messageCount, string(message.Topic), message.Value)
		}
	}

}

func main() {

	topic := flag.String("topic", "comments", "list of topics")

	urls := []string{"localhost:29092"}

	consumer, err := NewConsumer(urls)
	if err != nil {
		panic(err)
	}

	if err := consumer.AddTopic(*topic, 0, sarama.OffsetOldest); err != nil {
		panic(err)
	}

	fmt.Println("consumer started")

	quit := make(chan os.Signal, 2)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	consumer.Run()

	<-quit

	fmt.Println("Processed", consumer.messageCount, "messages")
	if err := consumer.consumer.Close(); err != nil {
		panic(err)
	}
}
