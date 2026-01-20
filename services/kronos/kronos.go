package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type DataCell struct {
	Id   string   `json:"id,omitempty"`
	Cmd  string   `json:"cmd,omitempty"`
	Day  int32    `json:"day,omitempty"`
	Mdt  string   `json:"mdt,omitempty"`
	Keys []string `json:"keys"`
	Val  int      `json:"val"`
}

type Kronos struct {
	produser   *kafka.Producer
	msgChan    chan *kafka.Message
	eventsChan chan kafka.Event
	signalChan chan os.Signal
	stepDur    time.Duration
	sync.WaitGroup
	Topic string
}

func main() {
	p_topic := flag.String("topic", "calend", "calend topic")
	p_timer := flag.Int64("timer", 1000, "timer duration (ms)")
	flag.Parse()

	krn := &Kronos{
		Topic:      *p_topic,
		stepDur:    time.Duration(*p_timer * 1000000),
		msgChan:    make(chan *kafka.Message, 100),
		signalChan: make(chan os.Signal, 1),
	}
	signal.Notify(krn.signalChan, syscall.SIGINT, syscall.SIGTERM)

	conf := &kafka.ConfigMap{
		"bootstrap.servers":  "localhost:9091,localhost:9092,localhost:9093",
		"client.id":          "kronos",
		"enable.idempotence": true,
		"acks":               "all",
		"linger.ms":          0,
	}

	produser, err := kafka.NewProducer(conf)
	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}
	defer produser.Close()
	krn.produser = produser

	go printDelivered(produser)

	go krn.createMessages()

	go krn.closeMsgChan()

	for msg := range krn.msgChan {
		// fmt.Println("msg:", string(msg.Key), string(msg.Value))
		produser.Produce(msg, nil)
	}
	unflushed := produser.Flush(1000)

	fmt.Println("Done!", unflushed)
}

func printDelivered(p *kafka.Producer) {
	cnt := 0
	for ev := range p.Events() {
		switch et := ev.(type) {
		case *kafka.Message:
			m := et
			cnt++
			if m.TopicPartition.Error != nil {
				fmt.Printf("%d Delivery failed: %v\n", cnt, m.TopicPartition.Error)
			} else {
				// fmt.Printf("%s %s Delivered to topic %s [%d] at offset %v\n",
				// 	string(m.Key), string(m.Value), *m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
			}
		case kafka.Error:
			fmt.Printf("Kafka error: %v\n", ev)
			os.Exit(1)
		default:
			fmt.Printf("Ignored event: %s\n", ev)
		}
	}
	fmt.Println("Delivery channel closed.")
}

func (k *Kronos) createMessages() {
	var (
		cont bool     = true
		cnt  int32    = 0
		cell DataCell = DataCell{
			Cmd:  "calend",
			Day:  0,
			Keys: []string{"*", "c2025"},
			Val:  0,
		}
		// ts time.Time
	)
	val, err := json.Marshal(cell)
	if err != nil {
		fmt.Printf("Can't marshall data cell %v : %s\n", cell, err)
		return
	}
	top := &kafka.TopicPartition{Topic: &k.Topic, Partition: kafka.PartitionAny}
	k.produser.Produce(&kafka.Message{
		TopicPartition: *top,
		Value:          []byte(val),
	}, nil)
	cnt++
	time.Sleep(5 * k.stepDur)
	ticker := time.NewTicker(k.stepDur)
	expired := time.After(60 * k.stepDur)
	defer ticker.Stop()
	k.Add(1)
	for cont {
		select {
		case <-k.signalChan:
			cont = false
			fmt.Println("Terminated!")
		case <-expired:
			cont = false
			fmt.Println("Time elapsed!")
		case <-ticker.C:
			cell.Day = cnt
			cell.Keys[1] = time.Date(2025, 8, int(cnt), 0, 0, 0, 0, time.UTC).Format("2006-01-02")
			cell.Val = 0
			val, err := json.Marshal(cell)
			if err != nil {
				fmt.Printf("Can't marshall data cell %v : %s\n", cell, err)
				continue
			}
			top := &kafka.TopicPartition{Topic: &k.Topic, Partition: kafka.PartitionAny}
			k.produser.Produce(&kafka.Message{
				TopicPartition: *top,
				Value:          []byte(val),
				// Timestamp:      ts,
			}, nil)
			// fmt.Println(ts.Local().UnixMilli())
			cnt++
		}
	}
	k.Done()
}

func (k *Kronos) closeMsgChan() {
	time.Sleep(10 * k.stepDur)
	k.Wait()
	close(k.msgChan)
	fmt.Println("Message channel closed")
}
