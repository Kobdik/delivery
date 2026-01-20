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
	"sync/atomic"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
)

type DataCell struct {
	Id   string   `json:"id,omitempty"`
	Cmd  string   `json:"cmd,omitempty"`
	Day  int32    `json:"day,omitempty"`
	Mdt  string   `json:"mdt,omitempty"`
	Keys []string `json:"keys"`
	Val  int      `json:"val"`
}

type Outlay struct {
	obj   byte
	cells []DataCell
	tasks *list.List
	vals  map[string]int
}

type Outlet struct {
	sync.RWMutex
	sync.WaitGroup
	CalendTop string
	StocksTop string
	OutlayTop string
	Prob      float64
	termChan  chan bool
	produser  *kafka.Producer
	stepDur   time.Duration
	cells     []DataCell
	outlays   []Outlay
	random    *rand.Rand
	calday    atomic.Int32
	verb      bool
}

func main() {
	p_path := flag.String("path", "./outlay.json", "path to tasks json file")
	p_calend := flag.String("calend", "calend", "consumed calend topic")
	p_outlay := flag.String("out", "outlet", "consumed outlay topic")
	p_stocks := flag.String("dem", "stocks", "producer's topic")
	p_timer := flag.Int64("timer", 1000, "timer duration (ms)")
	p_prob := flag.Float64("prob", 1.0, "probability")
	p_ling := flag.Int("ling", 15, "linger (ms)")
	p_verb := flag.Bool("verb", false, "verb to std output")
	flag.Parse()

	out := &Outlet{
		CalendTop: *p_calend,
		OutlayTop: *p_outlay,
		StocksTop: *p_stocks,
		Prob:      *p_prob,
		termChan:  make(chan bool),
		// cellChan: make(chan *DataCell, 4000),
		stepDur: time.Duration(*p_timer * 1000000),
		random:  rand.New(rand.NewSource(time.Now().UnixNano())),
		outlays: make([]Outlay, 4),
		verb:    *p_verb,
	}

	out.loadCells(*p_path)

	conf_prod := &kafka.ConfigMap{
		"bootstrap.servers":  "localhost:9092",
		"client.id":          "outlet",
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
	out.produser = producer

	go printDelivered(producer)

	for i := range []byte("1234") {
		out.Add(1)
		go out.readMessages(i)
	}

	go out.waitTerm()

	out.Wait()
	unflushed := producer.Flush(1000)

	for i, j := range []byte("1234") {
		fmt.Printf("Object-%c:\n", j)
		for mat, val := range out.outlays[i].vals {
			fmt.Printf("Out of %s is\t%d\n", mat, val)
		}
	}

	fmt.Println("Done!", unflushed)
}

func (s *Outlet) waitTerm() {
	signChan := make(chan os.Signal, 1)
	signal.Notify(signChan, syscall.SIGINT, syscall.SIGTERM)
	expired := time.After(12*time.Second + 60*s.stepDur)
	select {
	case <-signChan:
		fmt.Println("Terminated!")
		close(s.termChan)
	case <-expired:
		fmt.Println("Time elapsed!")
		close(s.termChan)
	}
}

func (s *Outlet) loadCells(path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		fmt.Printf("Can't read %s : %s", path, err)
		return
	}

	all_cells := make([]DataCell, 0, 4096)
	err = json.Unmarshal(data, &all_cells)
	if err != nil {
		fmt.Printf("Can't parse %s : %s", path, err)
		return
	}
	fmt.Println("all cells length:", len(all_cells))
	s.cells = all_cells

	for i, j := range []byte("1234") {
		s.outlays[i].obj = j
		cells := make([]DataCell, 0, 1024)
		for _, cell := range all_cells {
			// c.Keys[2] is like "t-1-2-3" - project1-object2-task3
			if cell.Keys[2][4] == j {
				// generate Id
				cell.Id = uuid.NewString()
				cell.Cmd = "demand"
				cells = append(cells, cell)
			}
		}
		s.outlays[i].cells = cells
		fmt.Printf("Object-%c cells length %d\n", j, len(cells))
	}
}

func (s *Outlet) initTasks(i int) {
	outlay := &s.outlays[i]
	// sort by Key[1] asc
	tasks := list.New()
	start_time := time.Now()
	for _, cell := range outlay.cells {
		back := tasks.Back()
		// if cell less than back then move cell to top
		for back != nil && cell.Keys[1] < back.Value.(DataCell).Keys[1] {
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
	fmt.Printf("Sort time J%c: %d microseconds\n", outlay.obj, elapsed.Microseconds())
	// initialize
	outlay.tasks = tasks
	outlay.vals = map[string]int{
		"cem":   0,
		"sand":  0,
		"stone": 0,
	}
}

func (s *Outlet) readMessages(i int) {
	var j byte = s.outlays[i].obj
	// different group.id
	conf_cons := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9091,localhost:9092,localhost:9093",
		"client.id":         fmt.Sprintf("cal-cid-%c", j),
		"group.id":          fmt.Sprintf("cal-gid-%c", j),
		"auto.offset.reset": "latest",
	}
	// "auto.offset.reset": "earliest",
	// independent consumer within own group
	consumer, err := kafka.NewConsumer(conf_cons)
	if err != nil {
		fmt.Printf("Failed to create consumer for %c: %s\n", j, err)
		return
	}
	defer consumer.Close()
	// subscribe to calend topic
	err = consumer.SubscribeTopics([]string{s.CalendTop, s.OutlayTop}, nil)
	if err != nil {
		fmt.Printf("Instance-%c failed to subscribe to %s and %s topics, cause: %s\n", j, s.CalendTop, s.OutlayTop, err)
		return
	}
	fmt.Printf("Instance J%c subscribed to %s and %s topics\n", j, s.CalendTop, s.OutlayTop)
	// read outlet's tasks

	var (
		cell  DataCell
		cont  bool  = true
		parts []int = make([]int, 4)
		day   int32
	)
	for cont {
		select {
		case <-s.termChan:
			cont = false
			fmt.Printf("Reader instance J%c terminated!\n", j)
		default:
			// messages read in parallel
			msg, err := consumer.ReadMessage(5 * s.stepDur)
			if err != nil {
				// fmt.Printf("Instance-%c failed to read message, cause %s\n", j, err)
				continue
			}
			// fmt.Printf("Instance %s topic %s[%d]-%d read %s\n", vnd, *msg.TopicPartition.Topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset, string(msg.Value))
			cell = DataCell{}
			err = json.Unmarshal(msg.Value, &cell)
			if err != nil {
				fmt.Printf("Can't parse data cell : %s", err)
				continue
			}
			switch cell.Cmd {
			case "calend":
				if len(cell.Keys) < 2 {
					fmt.Println("Not appropriate DataCell!")
					continue
				}
				// reset stores
				if cell.Day == 0 {
					if s.calday.Load() != 0 {
						day = 0
						// set only once
						s.calday.Store(day)
					}
					time.Sleep(2 * s.stepDur)
					s.initTasks(i)
					continue
				}
				// ensure true sequence
				if s.calday.CompareAndSwap(cell.Day-1, cell.Day) {
					// first event win
					day = s.calday.Load()
				}
				if s.verb {
					// fmt.Printf("Instance-%d day: %d\n", i, cell.Day)
					fmt.Printf("Instance-%d topic calend[%d] day %d, timestamp %d, time %d (ms)\n",
						msg.TopicPartition.Partition, i, day,
						msg.Timestamp.Local().UnixMilli(), time.Now().UnixMilli())
				}
				// time.Sleep(20 * time.Millisecond)
				if cell.Day != day {
					day = s.calday.Load()
				}
				if cell.Day == day {
					s.processCalend(cell, i)
				} else {
					fmt.Printf("Instance-%d topic calend[%d] cell's day %d differ from current day %d\n",
						msg.TopicPartition.Partition, i, day, cell.Day)
				}
			case "outlay":
				if len(cell.Keys) < 5 {
					fmt.Println("Not appropriate DataCell!")
					continue
				}
				// j => i
				if cell.Keys[2][4] == j {
					s.processOutlay(cell, i)
				}
				parts[msg.TopicPartition.Partition] += 1
			default:
				fmt.Printf("Unknown command %v in cell\n", cell)
				continue
			}
		}
	}
	if i == 0 {
		fmt.Println("Partitions used:")
		for i, p := range parts {
			fmt.Printf("%d: %d\n", i, p)
		}
	}
	s.Done()
}

// not used
func (s *Outlet) readOutlay(i int) {
	// different group.id
	conf_cons := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9091,localhost:9092,localhost:9093",
		"client.id":         fmt.Sprintf("out-cid-%d", i),
		"group.id":          "outlet-grp",
		"auto.offset.reset": "latest",
	}
	// "auto.offset.reset": "earliest",
	// independent consumer within own group
	consumer, err := kafka.NewConsumer(conf_cons)
	if err != nil {
		fmt.Printf("Failed to create consumer for %d: %s\n", i, err)
		return
	}
	defer consumer.Close()
	// subscribe to ticket topic
	// out := fmt.Sprintf("out%c", j)
	err = consumer.Subscribe(s.OutlayTop, nil)
	if err != nil {
		fmt.Printf("Instance-%d failed to subscribe to %s topic, cause: %s\n", i, s.OutlayTop, err)
		return
	}
	fmt.Printf("Instance J%d subscribed to %s topic\n", i, s.OutlayTop)
	// read outlet's tasks
	var (
		cell  DataCell
		cont  bool = true
		ind   int
		parts []int = make([]int, 4)
	)
	for cont {
		select {
		case <-s.termChan:
			cont = false
			fmt.Printf("Reader instance J%d terminated!\n", i)
		default:
			// messages read in parallel
			msg, err := consumer.ReadMessage(5 * s.stepDur)
			if err != nil {
				// fmt.Printf("Instance-%c failed to read message, cause %s\n", j, err)
				continue
			}
			parts[msg.TopicPartition.Partition] += 1
			// fmt.Printf("Instance %s topic %s[%d]-%d read %s\n", vnd, *msg.TopicPartition.Topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset, string(msg.Value))
			cell = DataCell{}
			err = json.Unmarshal(msg.Value, &cell)
			if err != nil {
				fmt.Printf("Can't parse data cell : %s", err)
				continue
			}
			switch cell.Cmd {
			case "outlay":
				ind = int(cell.Keys[2][4] - '1')
				s.processOutlay(cell, ind)
			default:
				fmt.Printf("Unknown command %v in cell\n", cell)
				continue
			}
		}
	}
	for i, p := range parts {
		fmt.Printf("%d: %d\n", i, p)
	}
	s.Done()
}

func (c *DataCell) readTask(task *list.Element, mdt string) bool {
	// copy task to cell
	*c = task.Value.(DataCell)
	if c.Keys[1] <= mdt {
		return true
	}
	return false
}

func (s *Outlet) processCalend(cell DataCell, i int) {
	var (
		cnt    int32      = 0
		day    int32      = cell.Day
		mdt    string     = cell.Keys[1]
		outlay *Outlay    = &s.outlays[i]
		tasks  *list.List = outlay.tasks
	)
	if tasks == nil {
		fmt.Println("No tasks created!")
		return
	}
	// fmt.Println(cell.Val, cell.Keys)
	// mark tasks and send demand
	for task := tasks.Front(); task != nil && cell.readTask(task, mdt); task = task.Next() {
		if cnt > 99 {
			fmt.Printf("Instance %d day %d: Too many demands send!\n", i, day)
			break
		}
		// fmt.Printf("%s catch Cal: %s, Vnd: %s, Mat: %s, Store: %s, Val: %d\n", mdt, cell.Keys[1], cell.Keys[2], cell.Keys[3], cell.Keys[4], cell.Val)
		if s.random.Float64() < s.Prob {
			// marshalled date
			cell.Day = day
			cell.Mdt = mdt
			task.Value = cell
			val, err := json.Marshal(cell)
			if err != nil {
				fmt.Printf("Can't marshall data cell %v : %s\n", cell, err)
				continue
			}
			// create demand event in stocks topic with key
			top := &kafka.TopicPartition{Topic: &s.StocksTop, Partition: kafka.PartitionAny}
			s.produser.Produce(&kafka.Message{
				TopicPartition: *top,
				Key:            fmt.Appendf([]byte{}, "key-%s", cell.Keys[4]),
				Value:          val,
			}, nil)
			cnt++
		}
	}
}

// clean task list
func (s *Outlet) processOutlay(cell DataCell, ind int) {
	var (
		id     string     = cell.Id
		outlay *Outlay    = &s.outlays[ind]
		tasks  *list.List = outlay.tasks
		mdt    string
		// ok     bool
	)
	if tasks == nil {
		return
	}
	mdt = cell.Mdt
	// traverse tasks from beginning
	for task := tasks.Front(); task != nil && cell.readTask(task, mdt); task = task.Next() {
		if cell.Id == id {
			// count and delete task
			outlay.vals[cell.Keys[3]] += cell.Val
			tasks.Remove(task)
			break
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
				// fmt.Printf("%d Message with key %s delivered to partition [%d]\n%v\n",
				// 	cnt, string(m.Key), m.TopicPartition.Partition, string(m.Value))
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
