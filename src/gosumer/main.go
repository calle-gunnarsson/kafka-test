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

	valueSchema, err := ioutil.ReadFile("/value_schema.avsc")
	checkErr(err, "Unable to read value_schema")

	valueCodec, err := goavro.NewCodec(string(valueSchema))
	checkErr(err, "Unable to create valueCodec")

	keySchema, err := ioutil.ReadFile("/key_schema.avsc")
	checkErr(err, "Unable to read key_schema")

	keyCodec, err := goavro.NewCodec(string(keySchema))
	checkErr(err, "Unable to create valueCodec")

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
				decoded, err := valueCodec.Decode(bb)
				checkErr(err, "Unable to decode value")
				record := decoded.(*goavro.Record)

				bb = bytes.NewBuffer(e.Key)
				decoded, err = keyCodec.Decode(bb)
				checkErr(err, "Unable to decode key")
				key := decoded.(string)

				f, err := record.Get("id")
				checkErr(err, "Unable get id from record")
				adID := f.(int32)

				f, err = record.Get("subject")
				checkErr(err, "Unable get subject from record")
				adSubject := f.(string)

				f, err = record.Get("price")
				checkErr(err, "Unable get price from record")
				adPrice := f.(int32)

				tp := e.TopicPartition
				fmt.Printf("%s[%d]@%-4d - %-6s - id: %-4d subject: %-7s price: %-6d\n", *tp.Topic, tp.Partition, tp.Offset, key, adID, adSubject, adPrice)
				if err != nil {
					fmt.Fprintf(os.Stderr, "%% Error: %v\n", err)
				}
			case kafka.PartitionEOF:
				fmt.Printf("%% Reached %v\n", e)

			case kafka.Error:
				fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
				run = false

			}
		}
	}

	fmt.Printf("Closing consumer\n")
	c.Close()
}
