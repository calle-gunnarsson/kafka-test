package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"syscall"

	"github.com/linkedin/goavro"

	"gopkg.in/confluentinc/confluent-kafka-go.v0/kafka"
)

func checkErr(err error, msg string) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "% %s: %v\n", msg, err)
		os.Exit(1)
	}
}

func main() {
	broker := "kafka:9092"
	group := "consumers"
	client := "gosumer"
	topic := "foobar"

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	schema, err := ioutil.ReadFile("/schema.avsc")
	checkErr(err, "Unable to read schema")

	codec, err := goavro.NewCodec(string(schema))
	checkErr(err, "Unable to create codec")

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":               broker,
		"client.id":                       client,
		"group.id":                        group,
		"session.timeout.ms":              6000,
		"go.events.channel.enable":        true,
		"go.application.rebalance.enable": true,
		"default.topic.config": kafka.ConfigMap{
			"auto.offset.reset":   "earliest",
			"offset.store.method": "broker",
		},
	})

	checkErr(err, "Unable to create consumer")

	fmt.Printf("Created Consumer %s\n", c)

	c.Subscribe(topic, nil)
	checkErr(err, "Unable to subscribe to topic")

	run := true
	for run == true {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false

		case ev := <-c.Events():
			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				fmt.Fprintf(os.Stderr, "%% %v\n", e)
				c.Assign(e.Partitions)

			case kafka.RevokedPartitions:
				fmt.Fprintf(os.Stderr, "%% %v\n", e)
				c.Unassign()

			case *kafka.Message:
				bb := bytes.NewBuffer(e.Value)
				decoded, err := codec.Decode(bb)
				record := decoded.(*goavro.Record)

				f, err := record.Get("id")
				adID := f.(int32)
				checkErr(err, "Unable get id from record")

				f, err = record.Get("subject")
				adSubject := f.(string)
				checkErr(err, "Unable get subject from record")

				f, err = record.Get("price")
				adPrice := f.(int32)
				checkErr(err, "Unable get price from record")

				tp := e.TopicPartition
				fmt.Printf("%s[%d]@%-4d - %-6s - id: %-4d subject: %-7s price: %-6d\n", *tp.Topic, tp.Partition, tp.Offset, e.Key, adID, adSubject, adPrice)
				if err != nil {
					fmt.Fprintf(os.Stderr, "%% Error: %v\n", err)
				}
			case kafka.PartitionEOF:
				//fmt.Printf("%% Reached %v\n", e)

			case kafka.Error:
				fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
				run = false

			}
		}
	}

	fmt.Printf("Closing consumer\n")
	c.Close()
}
