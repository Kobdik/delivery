package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

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

type Kadmin struct {
	topics  Topics
	client  *kafka.AdminClient
	oper    string
	pnum    int
	rnum    int
	incAuth bool
}

func main() {
	var topics Topics
	p_oper := flag.String("oper", "desc", "describe topics")
	p_pnum := flag.Int("pn", 1, "partitions number")
	p_rnum := flag.Int("rn", 1, "replication factor")
	flag.Var(&topics, "t", "-t calend -t supply")
	flag.Parse()

	if len(topics) == 0 {
		fmt.Println("No topics defined!")
		return
	}

	// Create a new AdminClient.
	admin, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9091,localhost:9092,localhost:9093",
	})
	if err != nil {
		fmt.Printf("Failed to create Admin client: %s\n", err)
		os.Exit(1)
	}
	defer admin.Close()

	kadm := &Kadmin{
		client:  admin,
		incAuth: true,
		topics:  topics,
		oper:    *p_oper,
		pnum:    *p_pnum,
		rnum:    *p_rnum,
	}
	fmt.Println(kadm.oper, kadm.topics)

	// Call DescribeTopics.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	switch *p_oper {
	case "add":
		kadm.createTopics(ctx, 8*time.Second)
	case "desc":
		kadm.describeTopics(ctx)
	case "del":
		kadm.deleteTopics(ctx, 8*time.Second)
	default:
		fmt.Println("No operation defined.")
	}

	<-time.After(5 * time.Second)
	fmt.Println("Done.")
}

func (k *Kadmin) createTopics(ctx context.Context, dur time.Duration) {
	topicSpecs := []kafka.TopicSpecification{}
	for _, topic := range k.topics {
		topicSpecs = append(topicSpecs, kafka.TopicSpecification{
			Topic:             topic,
			NumPartitions:     k.pnum,
			ReplicationFactor: k.rnum,
		})
	}

	results, err := k.client.CreateTopics(
		ctx, topicSpecs,
		kafka.SetAdminOperationTimeout(dur),
	)
	if err != nil {
		fmt.Printf("Failed to delete topics: %v\n", err)
		return
	}

	// Print results
	for _, result := range results {
		fmt.Printf("%s\n", result)
	}

	fmt.Println("Created", k.topics)
}

func (k *Kadmin) deleteTopics(ctx context.Context, dur time.Duration) {
	results, err := k.client.DeleteTopics(
		ctx, k.topics,
		kafka.SetAdminOperationTimeout(dur),
	)
	if err != nil {
		fmt.Printf("Failed to delete topics: %v\n", err)
		return
	}

	// Print results
	for _, result := range results {
		fmt.Printf("%s\n", result)
	}

	fmt.Println("Deleted", k.topics)
}

func (k *Kadmin) describeTopics(ctx context.Context) {
	topicsResult, err := k.client.DescribeTopics(
		ctx, kafka.NewTopicCollectionOfTopicNames(k.topics),
		kafka.SetAdminOptionIncludeAuthorizedOperations(k.incAuth))
	if err != nil {
		fmt.Printf("Failed to describe topics: %s\n", err)
		return
	}
	fmt.Printf("A total of %d topic(s) described:\n", len(topicsResult.TopicDescriptions))

	for _, t := range topicsResult.TopicDescriptions {
		if t.Error.Code() != 0 {
			fmt.Printf("Topic: %s has error: %s\n", t.Name, t.Error)
			continue
		}
		fmt.Printf("Topic: %s has succeeded\n", t.Name)
		fmt.Printf("Topic Id: %s\n", t.TopicID)
		if k.incAuth {
			fmt.Printf("Allowed operations: %s\n", t.AuthorizedOperations)
		}
		for i := 0; i < len(t.Partitions); i++ {
			fmt.Printf("\tPartition id: %d with leader: %s\n",
				t.Partitions[i].Partition, t.Partitions[i].Leader)
			fmt.Printf("\t\tThe in-sync replica count is: %d, they are: %s\n",
				len(t.Partitions[i].Isr), t.Partitions[i].Isr)
			fmt.Printf("\t\tThe replica count is: %d, they are: %s\n",
				len(t.Partitions[i].Replicas), t.Partitions[i].Replicas)
		}
	}
	fmt.Println("Described", k.topics)
}
