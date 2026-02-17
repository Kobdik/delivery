package main

import (
	"container/list"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/Kobdik/delivery/services/common"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// supplier
type Vendor struct {
	vnd   string
	tasks *list.List
	ids   map[string]string
	vals  map[string]int
}

type Supplier struct {
	// sync.RWMutex
	sync.WaitGroup
	Calend   string
	Supply   string
	prob     float64
	termChan chan bool
	produser *kafka.Producer
	stepDur  time.Duration
	cells    []common.DataCell
	random   *rand.Rand
	vendors  []Vendor
}

const B22_mask = 1<<22 - 1

func main() {
	p_path := flag.String("path", "./sup_tasks.json", "path to tasks json file")
	p_cal := flag.String("cal", "calend", "consumed calend topic")
	p_sup := flag.String("sup", "stocks", "producer's topic")
	p_timer := flag.Int64("timer", 1000, "timer duration (ms)")
	p_prob := flag.Float64("prob", 0.75, "probability")
	p_ling := flag.Int("ling", 7, "linger 7-10 (ms)")
	flag.Parse()

	dt := time.Date(2025, 8, 55, 0, 0, 0, 0, time.UTC)
	fmt.Println(dt.Format("2006-01-02"), dt)

	sup := &Supplier{
		Calend:   *p_cal,
		Supply:   *p_sup,
		prob:     *p_prob,
		termChan: make(chan bool),
		stepDur:  time.Duration(*p_timer * 1000000),
		random:   rand.New(rand.NewSource(time.Now().UnixNano())),
		vendors:  make([]Vendor, 2),
	}
	// load supply schedule
	sup.loadCells(*p_path)

	conf_prod := &kafka.ConfigMap{
		"bootstrap.servers":  "localhost:9091,localhost:9092,localhost:9093",
		"client.id":          "supplier",
		"enable.idempotence": true,
		"acks":               "all",
		"batch.size":         1024,
		"linger.ms":          *p_ling,
	}

	producer, err := kafka.NewProducer(conf_prod)
	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}
	defer producer.Close()
	sup.produser = producer

	go printDelivered(producer)

	vnds := []string{"vnd1", "vnd2"}
	for i, vnd := range vnds {
		sup.vendors[i].vnd = vnd
		sup.Add(1)
		go sup.readMessages(i)
	}

	go sup.waitTerm()

	sup.Wait()
	unflushed := producer.Flush(1000)

	for i, vnd := range vnds {
		fmt.Printf("Supplier %s:\n", vnd)
		for mat, val := range sup.vendors[i].vals {
			fmt.Printf("Supplied %d of %s\n", val, mat)
		}
	}

	fmt.Println("Done!", unflushed)
}

func (s *Supplier) waitTerm() {
	signChan := make(chan os.Signal, 1)
	signal.Notify(signChan, syscall.SIGINT, syscall.SIGTERM)
	expired := time.After(5*time.Second + 60*s.stepDur)
	select {
	case <-signChan:
		fmt.Println("Terminated!")
		close(s.termChan)
	case <-expired:
		fmt.Println("Time elapsed!")
		close(s.termChan)
	}
}

func (s *Supplier) loadCells(path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		fmt.Printf("Can't read %s : %s", path, err)
		return
	}

	cells := make([]common.DataCell, 0, 100)
	err = json.Unmarshal(data, &cells)
	if err != nil {
		fmt.Printf("Can't parse %s : %s", path, err)
		return
	}
	fmt.Println("cells length:", len(cells))
	s.cells = cells
}

func (s *Supplier) readTasks(vnd string) *list.List {
	cells := make([]common.DataCell, 0, 50)
	for _, cell := range s.cells {
		if vnd == cell.Keys[2] {
			cells = append(cells, cell)
		}
	}
	fmt.Printf("Vendor %s cells length %d\n", vnd, len(cells))
	// sort by Key[1] asc
	tasks := list.New()
	start_time := time.Now()
	for _, cell := range cells {
		back := tasks.Back()
		// if cell less than back then move cell to top
		for back != nil && cell.Keys[1] < back.Value.(common.DataCell).Keys[1] {
			back = back.Prev()
		}
		if back == nil {
			tasks.PushFront(cell)
		} else {
			tasks.InsertAfter(cell, back)
		}
	}
	last_time := time.Now()
	elapsed := last_time.Sub(start_time)
	fmt.Printf("Sort time: %d microseconds\n", elapsed.Microseconds())
	// sorted list
	return tasks
}

func (s *Supplier) readMessages(inst int) {
	vnd := s.vendors[inst].vnd
	// init values
	s.initVendor(inst)
	// different group.id
	conf_cons := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9091,localhost:9092,localhost:9093",
		"client.id":         fmt.Sprintf("sup-cid-%s", vnd),
		"group.id":          fmt.Sprintf("sup-gid-%s", vnd),
		"auto.offset.reset": "latest",
	}
	// "auto.offset.reset": "earliest",
	// independent consumer within own group
	consumer, err := kafka.NewConsumer(conf_cons)
	if err != nil {
		fmt.Printf("Failed to create consumer for %s: %s\n", vnd, err)
		return
	}
	defer consumer.Close()
	// subscribe to calend topic
	err = consumer.Subscribe(s.Calend, nil)
	if err != nil {
		fmt.Printf("Instance for %s failed to subscribe to topic %s, cause: %s\n", vnd, s.Calend, err)
		return
	}
	var (
		cell common.DataCell
		cont bool = true
		ts   int64
	)
	for cont {
		select {
		case <-s.termChan:
			cont = false
			fmt.Printf("Reader instance %s terminated!\n", vnd)
		default:
			cell = common.DataCell{}
			// calend - impulse for tasks reading
			msg, err := consumer.ReadMessage(10 * s.stepDur)
			if err != nil {
				// fmt.Printf("Instance %s failed to read message, cause %s\n", vnd, err)
				continue
			}
			err = json.Unmarshal(msg.Value, &cell)
			if err != nil {
				fmt.Printf("Can't parse data cell : %s", err)
				continue
			}
			// fmt.Printf("Instance %s topic %s[%d]-%d read %s\n", vnd, *msg.TopicPartition.Topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset, string(msg.Value))
			if cell.Day > 0 {
				ts = msg.Timestamp.UnixMicro() & B22_mask
				fmt.Printf("%2d:%s:%7d:%7d\n", cell.Day, vnd,
					msg.Timestamp.UnixMicro()&B22_mask, ts)
				// execute tasks
				s.popTasks(cell, inst)
			}
		}
	}
	s.Done()
}

func (s *Supplier) initVendor(inst int) {
	var vendor *Vendor = &s.vendors[inst]
	// create vendor's vals
	vendor.vals = map[string]int{
		"cem":   0,
		"sand":  0,
		"stone": 0,
	}
	// create vendor's tasks
	vendor.tasks = s.readTasks(vendor.vnd)
}

func (s *Supplier) popTasks(cell common.DataCell, inst int) {
	var (
		day        int32      = cell.Day
		mdt        string     = cell.Keys[1]
		vendor     *Vendor    = &s.vendors[inst]
		tasks      *list.List = vendor.tasks
		task, next *list.Element
	)
	if cell.Cmd != "calend" && len(cell.Keys) < 2 {
		fmt.Println("Not appropriate DataCell!")
		return
	}
	if tasks == nil {
		// ignore until initialized
		return
	}
	// traverse tasks from beginning
	for task = tasks.Front(); task != nil && cell.ReadTask(task, mdt); task = next {
		next = task.Next()
		if s.random.Float64() < s.prob {
			// fmt.Printf("%s catch Cal: %s, Vnd: %s, Mat: %s, Store: %s, Val: %d\n", sdt, cell.Keys[1], cell.Keys[2], cell.Keys[3], cell.Keys[4], cell.Val)
			cell.Cmd = "supply"
			// marshalled date
			cell.Day = day
			cell.Mdt = mdt
			val, err := json.Marshal(cell)
			if err != nil {
				fmt.Printf("Can't marshall data cell %v : %s\n", cell, err)
				continue
			}
			// create supply event with key
			top := &kafka.TopicPartition{Topic: &s.Supply, Partition: kafka.PartitionAny}
			s.produser.Produce(&kafka.Message{
				TopicPartition: *top,
				Key:            fmt.Appendf([]byte{}, "key-%s", cell.Keys[4]),
				Value:          val,
			}, nil)
			tasks.Remove(task)
			// inc supplied materials
			vendor.vals[cell.Keys[3]] += cell.Val
		}
	}
}

func printDelivered(p *kafka.Producer) {
	cnt := 0
	for ev := range p.Events() {
		switch m := ev.(type) {
		case *kafka.Message:
			if m.TopicPartition.Error != nil {
				fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
			} else {
				// msg counter
				cnt++
				// fmt.Printf("%d Message with key %s delivered to partition [%d]-%d\n%v\n",
				// 	cnt, string(m.Key), m.TopicPartition.Partition, m.TopicPartition.Offset, string(m.Value))
			}
		case kafka.Error:
			fmt.Printf("Kafka error: %v\n", ev)
		default:
			fmt.Printf("Ignored event: %s\n", ev)
		}
	}
	fmt.Println("Delivery channel closed.")
}
