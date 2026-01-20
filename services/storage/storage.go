package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
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

type Topics []string

// String is an implementation of the flag.Value interface
func (imp *Topics) String() string {
	return fmt.Sprintf("%v", *imp)
}

// Set is an implementation of the flag.Value interface
func (imp *Topics) Set(value string) error {
	*imp = append(*imp, value)
	return nil
}

type Stock struct {
	// sync.RWMutex
	Mat  map[string]int
	cal  chan *DataCell
	cnt  int32
	cost int
}

type Storage struct {
	sync.WaitGroup
	Topics    []string
	OutlayTop string
	SaldoPath string
	termChan  chan bool
	producer  *kafka.Producer
	stepDur   time.Duration
	saldo     []DataCell
	stocks    []Stock
	flow      chan *DataCell
	calday    atomic.Int32
	delays    int
	calc      bool
	verb      bool
}

func main() {
	var topics Topics
	flag.Var(&topics, "t", "-t calend -t stocks")
	p_timer := flag.Int64("timer", 1000, "timer duration (ms)")
	p_path := flag.String("path", "./saldo.json", "path to saldo json file")
	p_ling := flag.Int("ling", 15, "linger (ms)")
	p_clean := flag.Bool("clean", false, "clean topics")
	p_calc := flag.Bool("calc", false, "calculate integral cost")
	p_verb := flag.Bool("verb", false, "verbose output")
	flag.Parse()

	if len(topics) == 0 {
		fmt.Println("Define topics!")
		return
	}

	stg := &Storage{
		Topics:    topics,
		OutlayTop: "outlet",
		SaldoPath: *p_path,
		termChan:  make(chan bool),
		stepDur:   time.Duration(*p_timer * 1000000),
		stocks:    make([]Stock, 2),
		calc:      *p_calc,
		verb:      *p_verb,
	}

	go stg.waitTerm()

	if *p_clean {
		stg.cleanTopics()
		return
	}

	stg.loadSaldo()

	conf_prod := &kafka.ConfigMap{
		"bootstrap.servers":  "localhost:9091,localhost:9092,localhost:9093",
		"client.id":          "storage",
		"enable.idempotence": true,
		"acks":               "all",
		"batch.size":         8192,
		"linger.ms":          *p_ling,
	}

	producer, err := kafka.NewProducer(conf_prod)
	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}
	defer producer.Close()
	stg.producer = producer

	stg.Add(1)
	go stg.readCalend()

	for i := range 2 {
		stg.Add(1)
		go stg.readStocks(i)
	}

	stg.waitReaders()

	fmt.Println("Done!", stg.delays, len(stg.flow))
}

func (s *Storage) waitTerm() {
	signChan := make(chan os.Signal, 1)
	signal.Notify(signChan, syscall.SIGINT, syscall.SIGTERM)
	expired := time.After(10*time.Second + 60*s.stepDur)
	select {
	case <-signChan:
		fmt.Println("Terminated!")
	case <-expired:
		fmt.Println("Time elapsed!")
	}
	close(s.termChan)
}

func (s *Storage) waitReaders() {
	s.Wait()
	cost := 0
	unflushed := s.producer.Flush(2000)
	for i := range s.stocks {
		fmt.Printf("Stock %d:\n", i)
		for mat, val := range s.stocks[i].Mat {
			fmt.Printf("Reserve of %s is\t%d\n", mat, val)
		}
		cost += s.stocks[i].cost
	}
	if s.calc {
		fmt.Printf("Cost of stockpiles: %d\n", cost/30000)
	}
	fmt.Println("Stock readers finished", unflushed)
}

func (s *Storage) loadSaldo() {
	// executed only once
	data, err := os.ReadFile(s.SaldoPath)
	if err != nil {
		fmt.Printf("Can't read %s : %s", s.SaldoPath, err)
		return
	}

	s.saldo = make([]DataCell, 0, 6)
	err = json.Unmarshal(data, &s.saldo)
	if err != nil {
		fmt.Printf("Can't parse %s : %s", s.SaldoPath, err)
		return
	}
	fmt.Println("saldo length:", len(s.saldo))
}

func (c *DataCell) stIndex() (int, error) {
	// Keys[4] is st1 or st2
	switch c.Keys[4] {
	case "st1":
		return 0, nil
	case "st2":
		return 1, nil
	}
	return -1, fmt.Errorf("Invalid storage code: %s for cell %v\n", c.Keys[4], c)
}

func (s *Storage) initStocks() {
	for i := range 2 {
		s.stocks[i] = Stock{
			Mat: map[string]int{
				"cem":   0,
				"sand":  0,
				"stone": 0,
			},
			cal:  make(chan *DataCell, 100),
			cost: 0,
		}
	}
	s.delays = 0
	s.calday.Store(0)
	s.flow = make(chan *DataCell, 4000)
	for _, cell := range s.saldo {
		ind, err := cell.stIndex()
		if err != nil {
			fmt.Println(err.Error())
			continue
		}
		s.stocks[ind].Mat[cell.Keys[3]] += cell.Val
		s.flow <- &cell
	}

	for i := range s.stocks {
		fmt.Printf("Stock %d:\n", i)
		for mat, val := range s.stocks[i].Mat {
			fmt.Printf("Reserved %d of %s\n", val, mat)
		}
	}
}

func matCost(mat string, val int) int {
	var cost int = 0
	switch mat {
	case "cem":
		cost = val * 67
	case "sand":
		cost = val * 8
	case "stone":
		cost = val * 21
	}
	return cost
}

func (s *Storage) stockpileCost() {
	for i := range 2 {
		for mat, val := range s.stocks[i].Mat {
			s.stocks[i].cost += matCost(mat, val)
		}
	}
}

func (s *Storage) incrementStocks(cell *DataCell, i int, day int32) {
	if s.stocks[i].Mat == nil {
		fmt.Printf("Supply was ignored: %v\n", cell)
		return
	}
	// just warning
	if cell.Day != day {
		fmt.Printf("%d income: %d\n", day, cell.Day)
	}
	// fmt.Printf("%s : %s inc %s storage %s\n", cell.Mdt, cell.Keys[1], cell.Keys[3], cell.Keys[4])
	// s.stocks[i].Lock()
	s.stocks[i].Mat[cell.Keys[3]] += cell.Val
	// s.stocks[i].Unlock()
	if s.calc {
		s.stocks[i].cost += matCost(cell.Keys[3], cell.Val)
	}
	s.flow <- cell
}

func (s *Storage) processDemand(cell *DataCell, ind int) {
	// increment demands counter
	s.stocks[ind].cnt++
	// sure stok's reserve >= 0
	reserve := s.stocks[ind].Mat[cell.Keys[3]]
	if reserve < cell.Val {
		// fmt.Printf("%s Not enough %s in %s. Remains %d, but demands %d\n", mdt, cell.Keys[3], cell.Keys[4], reserve, cell.Val)
		s.delays++
		return
	}
	cell.Cmd = "outlay"
	top := &kafka.TopicPartition{Topic: &s.OutlayTop, Partition: kafka.PartitionAny}
	val, err := json.Marshal(*cell)
	if err != nil {
		fmt.Printf("Can't marshall outlay cell %v : %s\n", cell, err)
		return
	}
	s.producer.Produce(&kafka.Message{
		TopicPartition: *top,
		// Key:            []byte{cell.Keys[2][4]},
		Value: val,
	}, nil)
	// decrement stock's value
	s.stocks[ind].Mat[cell.Keys[3]] = reserve - cell.Val
	s.flow <- cell
}

func (s *Storage) readCalend() {
	// single instance
	conf_cons := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9091,localhost:9092,localhost:9093",
		"client.id":         "storage-cal",
		"group.id":          "storage-grp",
		"auto.offset.reset": "latest",
	}
	// calend consumer
	consumer, err := kafka.NewConsumer(conf_cons)
	if err != nil {
		fmt.Printf("Failed to create storage-cal consumer: %s\n", err)
		return
	}
	defer consumer.Close()
	// subscribe to calend topic
	err = consumer.Subscribe("calend", nil)
	if err != nil {
		fmt.Printf("Storage-cal failed to subscribe to calend topic, cause: %s\n", err)
		return
	}
	fmt.Println("Storage-cal subscribed to calend topic")
	var (
		cell *DataCell
		cont bool = true
	)
	for cont {
		select {
		case <-s.termChan:
			cont = false
			fmt.Println("Calend reader terminated!")
		default:
			msg, err := consumer.ReadMessage(2 * s.stepDur)
			if err != nil {
				fmt.Printf("Calend reader failed, cause %v\n", err)
				continue
			}
			// messages separated with key: Partition-Keys[2] pairs
			// fmt.Printf("%s Topic %s[%d]-%d read %s\n", msg.Key, *msg.TopicPartition.Topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset, string(msg.Value))
			cell = new(DataCell)
			err = json.Unmarshal(msg.Value, &cell)
			if err != nil {
				fmt.Printf("Can't parse : %s", err)
				continue
			}
			switch cell.Cmd {
			case "calend":
				if cell.Day == 0 {
					// prepare stocks
					s.initStocks()
					continue
				}
				// sequence of marshalled dates
				if !s.calday.CompareAndSwap(cell.Day-1, cell.Day) {
					if s.verb {
						fmt.Printf("Topic calend[%d] wrong sequense of days %d, %d timestamp %d, time %d (ms)\n",
							msg.TopicPartition.Partition, s.calday.Load(), cell.Day,
							msg.Timestamp.Local().UnixMilli(), time.Now().UnixMilli())
					} else {
						fmt.Printf("Topic calend[%d] wrong sequense of days %d, %d\n",
							msg.TopicPartition.Partition, s.calday.Load(), cell.Day)
					}
					continue
				}
				// demands counters
				s.stocks[0].cnt = 0
				s.stocks[1].cnt = 0
				if s.calc {
					s.stockpileCost()
				}
				if s.verb {
					fmt.Printf("Topic calend[%d] day %d, timestamp %d, time %d (ms)\n",
						msg.TopicPartition.Partition, cell.Day,
						msg.Timestamp.Local().UnixMilli(), time.Now().UnixMilli())
				}
				// send cell by ref
				s.stocks[0].cal <- cell
				s.stocks[1].cal <- cell
			default:
				if s.verb {
					fmt.Printf("Unknown command %v in cell\n", cell)
				}
				continue
			}
		}
	}
	s.Done()
	fmt.Println("Storage-cal done.")
}

func (s *Storage) readStocks(inst int) {
	// common group.id
	conf_cons := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9091,localhost:9092,localhost:9093",
		"client.id":         fmt.Sprintf("storage-%d", inst),
		"group.id":          "storage-grp",
		"auto.offset.reset": "latest",
	}
	// consumer within common group
	consumer, err := kafka.NewConsumer(conf_cons)
	if err != nil {
		fmt.Printf("Failed to create storage-%d consumer: %s\n", inst, err)
		return
	}
	defer consumer.Close()
	// subscribe to stocks topic
	err = consumer.Subscribe("stocks", nil)
	if err != nil {
		fmt.Printf("Storage-%d failed to subscribe to stocks topic, cause: %s\n", inst, err)
		return
	}
	fmt.Printf("Storage-%d subscribed to stocks topic\n", inst)
	var (
		cal  *DataCell
		cell *DataCell
		cont bool  = true
		day  int32 = 0
		ind  int
	)
	for cont {
		select {
		case <-s.termChan:
			cont = false
			fmt.Printf("Storage-%d terminated!\n", inst)
		case cal = <-s.stocks[inst].cal:
			// marshalled date
			day = cal.Day
		// stocks topic
		default:
			msg, err := consumer.ReadMessage(5 * s.stepDur)
			if err != nil {
				// fmt.Printf("Reader-%d failed, cause %v\n", inst, err)
				continue
			}
			// messages separated with key: Partition-Keys[2] pairs
			// fmt.Printf("%s Topic %s[%d]-%d read %s\n", msg.Key, *msg.TopicPartition.Topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset, string(msg.Value))
			cell = new(DataCell)
			err = json.Unmarshal(msg.Value, cell)
			if err != nil {
				fmt.Printf("Can't parse : %s", err)
				continue
			}
			switch cell.Cmd {
			case "supply":
				// too late or too early
				if cell.Day < day || day+1 < cell.Day {
					continue
				}
				ind, err = cell.stIndex()
				if err != nil {
					fmt.Println(err.Error())
					continue
				}
				if cell.Day > day || s.stocks[ind].cnt > 30 {
					day = s.calday.Load()
					if s.verb {
						fmt.Printf("%d Updated stocks[%d] day %d for supply %d\n",
							s.stocks[ind].cnt, msg.TopicPartition.Partition, day, cell.Day)
					}
				}
				// take all supplied materials as is
				if cell.Day != day {
					if s.verb {
						fmt.Printf("%d Storage-%d supply day %d differ from stocks[%d] day %d\n",
							s.stocks[ind].cnt, inst, cell.Day, msg.TopicPartition.Partition, day)
					}
				}
				// fmt.Printf("Instance-%d Stock %d\n", inst, ind)
				s.incrementStocks(cell, ind, day)
			case "demand":
				// too late or too early
				if cell.Day < day || day+1 < cell.Day {
					continue
				}
				ind, err = cell.stIndex()
				if err != nil {
					fmt.Println(err)
					continue
				}
				if cell.Day > day || s.stocks[ind].cnt > 30 {
					day = s.calday.Load()
					if s.verb {
						fmt.Printf("%d Updated stocks[%d] day %d for demand %d\n",
							s.stocks[ind].cnt, msg.TopicPartition.Partition, day, cell.Day)
					}
				}
				// check demands day
				if cell.Day != day {
					if s.verb {
						fmt.Printf("%d Storage-%d demand day %d differ from stocks[%d] day %d timestamp %d time %d (ms)\n",
							s.stocks[ind].cnt, inst, cell.Day, msg.TopicPartition.Partition, day,
							msg.Timestamp.Local().UnixMilli(), time.Now().UnixMilli())
					}
					continue
				}
				// check daily sent counter
				if s.stocks[ind].cnt > 60 {
					if s.verb {
						fmt.Println(cell.Mdt, "Too many demands!")
					}
					continue
				}
				// demands from outlet
				s.processDemand(cell, ind)
			default:
				if s.verb {
					fmt.Printf("Unknown command %v in cell\n", cell)
				}
				continue
			}

		}
	}
	s.Done()
	fmt.Printf("Reader-%d done.\n", inst)
}

func printDelivered(p *kafka.Producer) {
	cnt := 0
	for ev := range p.Events() {
		switch m := ev.(type) {
		case *kafka.Message:
			if m.TopicPartition.Error != nil {
				fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
			} else {
				fmt.Printf("%d Message with key %s delivered to partition [%d]\n%v\n",
					cnt, string(m.Key), m.TopicPartition.Partition, string(m.Value))
				cnt++
			}
		case kafka.Error:
			fmt.Printf("Kafka error: %v\n", ev)
		default:
			fmt.Printf("Ignored event: %s\n", ev)
		}
	}
	fmt.Println("Delivery channel closed.")
}

func (s *Storage) cleanTopics() {
	conf := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9091,localhost:9092,localhost:9093",
		"group.id":          "storage-grp",
		"auto.offset.reset": "latest",
	}
	consumer, err := kafka.NewConsumer(conf)
	if err != nil {
		fmt.Printf("Failed to create storage consumer: %s\n", err)
		return
	}
	defer consumer.Close()
	// subscribe to calend, supply, demand and outlay
	err = consumer.SubscribeTopics(s.Topics, nil)
	if err != nil {
		fmt.Printf("Storage failed to subscribe to topics %v, cause: %s\n", s.Topics, err)
		return
	}
	var parts []int = make([]int, 4)
	cont := true
	for cont {
		select {
		case <-s.termChan:
			cont = false
			fmt.Println("Reader terminated!")
		default:
			msg, err := consumer.ReadMessage(5 * time.Duration(s.stepDur))
			if err != nil {
				fmt.Printf("Reader failed, cause %v\n", err)
				continue
			}
			parts[msg.TopicPartition.Partition] += 1
			// messages separated with key: Partition-Keys[2] pairs
			// fmt.Printf("%s Topic %s[%d]-%d\n", msg.Key, *msg.TopicPartition.Topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset)
			fmt.Printf("%s Topic %s[%d]-%d read %s\n", msg.Key, *msg.TopicPartition.Topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset, string(msg.Value))
		}
	}
	for i, p := range parts {
		fmt.Printf("[%d]: %d\n", i, p)
	}

	fmt.Println("Cleaned")
}
