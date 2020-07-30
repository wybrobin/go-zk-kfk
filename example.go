package main

import (
	"fmt"
	"go-zk-kfk/zkkfk"
	"github.com/Shopify/sarama"
	"strconv"
	"time"
	"sync"
)

var gWg = sync.WaitGroup{}

func producer(addrs []string) {
	kfkConfig := sarama.NewConfig()
	kfkConfig.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(addrs, kfkConfig)
	if err != nil {
		panic(err)
	}

	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic: "test3",
	}

	value := 0
	for ;; value++ {
		msg.Value = sarama.ByteEncoder(strconv.Itoa(value))
		partition, offset, err := producer.SendMessage(msg)

		if err != nil {
			fmt.Println("SendMessage error=", err)
		} else {
			fmt.Printf("partition = %d, offset = %d, value = %d\n", partition, offset, value)
		}
		time.Sleep(time.Second * 5)
	}
}


func consumerPartition(partitionId int32, addrs []string)  {
	kfkConfig := sarama.NewConfig()

	consumer, err := sarama.NewConsumer(addrs, kfkConfig)
	if err != nil {
		panic(err)
	}

	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition("test3", partitionId, sarama.OffsetOldest)
	if err != nil {
		fmt.Println("ConsumePartition error = ", err)
		return
	}

	defer partitionConsumer.Close()

	for {
		select {
		case msg := <-partitionConsumer.Messages():
			fmt.Printf("msg partition: %d, offset: %d, timestamp: %d, value: %s\n", msg.Partition, msg.Offset, msg.Timestamp, string(msg.Value))
		case err := <-partitionConsumer.Errors():
			fmt.Println("consumer erorr=", err)
		}
	}

}

func consumer(addrs []string) {
	kfkConfig := sarama.NewConfig()

	consumer, err := sarama.NewConsumer(addrs, kfkConfig)
	if err != nil {
		panic(err)
	}

	defer consumer.Close()

	partitionIds, err := consumer.Partitions("test3")
	if err != nil {
		panic(err)
	}

	for _, partitionId := range partitionIds {
		go consumerPartition(partitionId, addrs)
	}
}

func main() {
	gWg.Add(1)
	kfkAddr := zkkfk.GetKafkaAddrs([]string{"127.0.0.1:2181", "127.0.0.1:2182"}, time.Second*3)
	fmt.Println(kfkAddr)

	go producer(kfkAddr)
	go consumer(kfkAddr)

	gWg.Wait()
}
