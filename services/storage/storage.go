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

	"github.com/Kobdik/delivery/services/common"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type FlowCell struct {
	common.DataCell
	Ts int64 `json:"ts,omitempty"`
}

// calend stocks
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
	cal  chan *FlowCell
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
	saldo     []FlowCell
	stocks    []Stock
	flow      chan *FlowCell
	calday    atomic.Int32
	delays    int
	calc      bool
	verb      bool
}

const (
	B20_mask = 1<<20 - 1
	B22_mask = 1<<22 - 1
)

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

	stg.initStocks()

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

	if stg.calc {
		stg.calcCost()
	}
	fmt.Printf("Done with %d delayed demands.\n", stg.delays)
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
	// flush produced
	unflushed := s.producer.Flush(2000)
	fmt.Println("Stock readers finished", unflushed)
	// read available
	close(s.flow)
}

func (s *Storage) calcCost() {
	cost := 0
	for i := range s.stocks {
		fmt.Printf("Stock %d:\n", i)
		for mat, val := range s.stocks[i].Mat {
			fmt.Printf("Reserve of %s is\t%d\n", mat, val)
		}
		cost += s.stocks[i].cost
	}
	cost = cost / 30000
	fmt.Printf("Cost of stockpiles: %d\n", cost)

	cells := make([]FlowCell, 0, 4000)
	for cell := range s.flow {
		cells = append(cells, *cell)
	}

	data, err := json.Marshal(cells)
	if err != nil {
		fmt.Printf("Can't marshall flow of cells : %s\n", err)
		return
	}

	err = os.WriteFile(fmt.Sprintf("Flow_%dcost_%ddelays.json", cost, s.delays), data, 0644)
	if err != nil {
		fmt.Println("Ошибка записи файла:", err)
	}
}

func (s *Storage) loadSaldo() {
	// executed only once
	data, err := os.ReadFile(s.SaldoPath)
	if err != nil {
		fmt.Printf("Can't read %s : %s", s.SaldoPath, err)
		return
	}

	s.saldo = make([]FlowCell, 0, 6)
	err = json.Unmarshal(data, &s.saldo)
	if err != nil {
		fmt.Printf("Can't parse %s : %s", s.SaldoPath, err)
		return
	}
	fmt.Println("saldo length:", len(s.saldo))
}

func (s *Storage) initStocks() {
	for i := range 2 {
		s.stocks[i] = Stock{
			Mat: map[string]int{
				"cem":   0,
				"sand":  0,
				"stone": 0,
			},
			cal:  make(chan *FlowCell, 100),
			cost: 0,
		}
	}
	s.delays = 0
	s.calday.Store(0)
	s.flow = make(chan *FlowCell, 4000)
	for _, cell := range s.saldo {
		ind, err := cell.StIndex()
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

func (s *Storage) incrementStocks(cell *FlowCell, ind int) {
	if s.stocks[ind].Mat == nil {
		fmt.Printf("Supply was ignored: %v\n", cell)
		return
	}
	// there is no concurent access
	s.stocks[ind].Mat[cell.Keys[3]] += cell.Val
	// increment materials cost
	if s.calc {
		s.stocks[ind].cost += matCost(cell.Keys[3], cell.Val)
	}
	cell.Ts = time.Now().UnixMicro() & B20_mask
	s.flow <- cell
}

func (s *Storage) processDemand(cell *FlowCell, ind int) {
	// increment demands counter
	s.stocks[ind].cnt++
	// sure stock's reserve >= 0
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
		// Key: []byte{cell.Keys[2][4]},
		Value: val,
	}, nil)
	// decrement stock's value
	s.stocks[ind].Mat[cell.Keys[3]] = reserve - cell.Val
	cell.Ts = time.Now().UnixMicro() & B20_mask
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
		cell *FlowCell
		cont bool = true
		// recv int64
		band int
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
			cell = new(FlowCell)
			err = json.Unmarshal(msg.Value, &cell)
			if err != nil {
				fmt.Printf("Can't parse : %s", err)
				continue
			}
			switch cell.Cmd {
			case "calend":
				if cell.Day == 0 {
					band = cell.Val
					continue
				}
				if cell.Val != band {
					continue
				}
				// recv = time.Now().UnixMicro() & B22_mask
				// sequence of days
				if !s.calday.CompareAndSwap(cell.Day-1, cell.Day) {
					fmt.Printf("Topic calend[%d] wrong sequense of days %d, %d timestamp %d, time %d (ms)\n",
						msg.TopicPartition.Partition, s.calday.Load(), cell.Day,
						msg.Timestamp.Local().UnixMilli(), time.Now().UnixMilli())
					continue
				}
				if s.stocks[0].cnt > 60 || s.stocks[1].cnt > 60 {
					fmt.Printf("Calend %d. Too many demands!\n", cell.Day-1)
				}
				// demands counters
				s.stocks[0].cnt = 0
				s.stocks[1].cnt = 0
				if s.calc {
					s.stockpileCost()
				}
				// fmt.Printf("calend %2d:%7d:%7d\n", cell.Day,
				// 	msg.Timestamp.UnixMicro()&B22_mask, recv)
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
		cal  *FlowCell
		cell *FlowCell
		cont bool  = true
		day  int32 = 0
		cts  int64
		ind  int
	)
	for cont {
		select {
		case <-s.termChan:
			cont = false
			fmt.Printf("Storage-%d terminated!\n", inst)
		case cal = <-s.stocks[inst].cal:
			// calend day
			day = cal.Day
			cts = time.Now().UnixMicro() & B22_mask
			fmt.Printf("%2d:stocks[%d]:%7d\n", day, inst, cts)
		// read stocks topic
		default:
			msg, err := consumer.ReadMessage(s.stepDur)
			if err != nil {
				// fmt.Printf("Reader-%d failed, cause %v\n", inst, err)
				continue
			}
			// messages separated with key: Partition-Keys[2] pairs
			// fmt.Printf("%s Topic %s[%d]-%d read %s\n", msg.Key, *msg.TopicPartition.Topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset, string(msg.Value))
			cell = new(FlowCell)
			err = json.Unmarshal(msg.Value, cell)
			if err != nil {
				fmt.Printf("Can't parse : %s", err)
				continue
			}
			switch cell.Cmd {
			case "supply":
				// too late
				if cell.Day < day {
					fmt.Printf("supply %d too late at %d\n", cell.Day, day)
					continue
				}
				ind, err = cell.StIndex()
				if err != nil {
					fmt.Println(err.Error())
					continue
				}
				// update day
				if s.stocks[ind].cnt == 0 {
					day = s.calday.Load()
					if s.verb {
						fmt.Printf("updated stocks[%d] supply %d at %d\n",
							msg.TopicPartition.Partition, cell.Day, day)
					}
				}
				// too early
				if day+1 < cell.Day {
					fmt.Printf("supply %d too early at %d\n", cell.Day, day)
					continue
				}
				// take all supplied materials as is
				if cell.Day != day {
					if s.verb {
						fmt.Printf("%d Storage-%d supply day %d differ from stocks[%d] day %d\n",
							s.stocks[ind].cnt, inst, cell.Day, msg.TopicPartition.Partition, day)
					} else {
						fmt.Printf("supply %d income at %d\n", cell.Day, day)
					}
				}
				// fmt.Printf("Instance-%d Stock %d\n", inst, ind)
				s.incrementStocks(cell, ind)
			case "demand":
				// ignore too late
				if cell.Day < day {
					continue
				}
				ind, err = cell.StIndex()
				if err != nil {
					fmt.Println(err)
					continue
				}
				// update day
				if s.stocks[ind].cnt%20 == 0 {
					day = s.calday.Load()
					if s.verb {
						fmt.Printf("updated stocks[%d] demand %d at %d\n",
							msg.TopicPartition.Partition, cell.Day, day)
					}
				}
				// ignore too early
				if day+1 < cell.Day {
					if s.verb && s.stocks[ind].cnt == 0 {
						fmt.Printf("too early stocks[%d] demand %d at %d\n",
							msg.TopicPartition.Partition, cell.Day, day)
					}
					continue
				}
				// check daily sent counter
				if s.stocks[ind].cnt > 60 {
					// too many demands!
					continue
				}
				// check demands day
				if cell.Day != day {
					if s.verb {
						fmt.Printf("Demands: %d. Storage-%d demand day %d differ from stocks[%d] day %d timestamp %d time %d (ms)\n",
							s.stocks[ind].cnt, inst, cell.Day, msg.TopicPartition.Partition, day,
							msg.Timestamp.Local().UnixMilli(), time.Now().UnixMilli())
					}
					// Storage demand day differ from stocks current day
					continue
				}
				// demands from outlet
				s.processDemand(cell, ind)
			default:
				fmt.Printf("Unknown command %v in cell\n", cell)
				continue
			}

		}
	}
	s.Done()
	fmt.Printf("Reader-%d done.\n", inst)
}

// clean artefacts
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
