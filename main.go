package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
)

const defaultStream = "stream1"
const defaultSubject = "fruits"
const defaultConsumer = "stream1-consumer"
const defaultDeliverySubject = "subject1"

func main() {
	mode := flag.String("mode", "", "'publish' or 'consume'")
	natsUrl := flag.String("nats-url", "", "NATS server URL")
	flag.Parse()

	if *mode != "consume" && *mode != "publish" {
		flag.Usage()
		return
	}

	if *mode == "publish" {
		publish(*natsUrl)
	} else if *mode == "consume" {
		consume(*natsUrl)
	}
}

func connectStream(natsUrl string) (*nats.Conn, nats.JetStreamContext, error) {
	if natsUrl == "" {
		natsUrl = nats.DefaultURL
	}

	nc, err := nats.Connect(natsUrl)
	if err != nil {
		return nil, nil, err
	}

	js, err := nc.JetStream()
	if err != nil {
		return nil, nil, err
	}

	return nc, js, nil
}

func publish(natsUrl string) error {
	_, js, err := connectStream(natsUrl)
	if err != nil {
		return fmt.Errorf("stream error: %s", err)
	}

	js.AddStream(&nats.StreamConfig{
		Name:     defaultStream,
		Subjects: []string{fmt.Sprintf("%s.>", defaultSubject)},
	})
	if err != nil {
		fmt.Printf("add stream error: %s\n", err.Error())
		return err
	}

	js.DeleteConsumer(defaultStream, "tropical")
	js.DeleteConsumer(defaultStream, "berry")

	js.AddConsumer(defaultStream, &nats.ConsumerConfig{
		Durable:        "tropical",
		DeliverSubject: "tropicalDelivery",
		DeliverGroup:   "tropical",
		AckPolicy:      nats.AckExplicitPolicy,
		MaxAckPending:  1,
		FilterSubject:  "fruits.tropical",
	})
	js.UpdateConsumer(defaultStream, &nats.ConsumerConfig{
		Durable:        "tropical",
		DeliverSubject: "tropicalDelivery",
		DeliverGroup:   "tropical",
		AckPolicy:      nats.AckExplicitPolicy,
		MaxAckPending:  100,
		FilterSubject:  "fruits.tropical",
	})

	js.AddConsumer(defaultStream, &nats.ConsumerConfig{
		Durable:        "berry",
		DeliverSubject: "berryDelivery",
		DeliverGroup:   "berry",
		AckPolicy:      nats.AckExplicitPolicy,
		MaxAckPending:  1,
		FilterSubject:  "fruits.berry",
	})
	js.UpdateConsumer(defaultStream, &nats.ConsumerConfig{
		Durable:        "berry",
		DeliverSubject: "berryDelivery",
		DeliverGroup:   "berry",
		AckPolicy:      nats.AckExplicitPolicy,
		MaxAckPending:  5,
		FilterSubject:  "fruits.berry",
	})

	for i := 1; i < 600; i++ {
		fruitCategories := []string{"melon", "tropical", "berry"}
		fruitCat := fruitCategories[i%len(fruitCategories)]
		subject := fmt.Sprintf("%s.%s", defaultSubject, fruitCat)
		_, err := js.Publish(subject, []byte(fmt.Sprintf("%s-%s-%d", "fruit", fruitCat, i)), nats.MsgId(fmt.Sprintf("%d-%d", i, time.Now())))
		if err != nil {
			fmt.Printf("publisher error: %s\n", err.Error())
			return err
		}
		fmt.Printf("published to subject %s, id %d..\n", subject, i)

		// time.Sleep(100 * time.Millisecond)
	}

	return nil
}

func consume(natsUrl string) error {
	// workerCount := 100
	// quit := make(chan int, workerCount)

	nc, _, err := connectStream(natsUrl)
	if err != nil {
		return fmt.Errorf("stream error: %s", err)
	}

	msgs := make(chan *nats.Msg, 10)
	go func() {
		for {
			msg := <-msgs
			go func(msg *nats.Msg) {
				fmt.Printf("received message %s\n", string(msg.Data))
				time.Sleep(1 * time.Second)
				msg.Ack()
				// fmt.Printf("acked message %s\n", string(msg.Data))
			}(msg)
		}
	}()

	tropicalSub, err := nc.QueueSubscribe("tropicalDelivery", "tropical", func(msg *nats.Msg) {
		msgs <- msg
	})
	if err != nil {
		fmt.Printf("error queue subscrbe: %s\n", err)
		return err
	}

	// nc.QueueSubscribe("berryDelivery", "berry", func(msg *nats.Msg) {
	// 	fmt.Printf("received message %s\n", string(msg.Data))

	// 	time.Sleep(1 * time.Second)

	// 	msg.Ack()
	// 	fmt.Printf("acked message %s\n", string(msg.Data))
	// })

	go func() {
		for {
			if c, err := tropicalSub.Dropped(); err != nil {
				fmt.Printf("error checking dropped: %s\n", err)
			} else {
				fmt.Printf("reporting dropped count: %d\n", c)
			}
			time.Sleep(2 * time.Second)
		}
	}()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT)
	<-sigs

	if err := tropicalSub.Drain(); err != nil {
		fmt.Printf("error draining: %s", err)
	}

	for {
		if c, _, err := tropicalSub.Pending(); err != nil {
			fmt.Printf("error checking draining: %s", err)
		} else if c <= 0 {
			fmt.Println("no more pending message")
			break
		}
		time.Sleep(1 * time.Second)
	}

	return nil
}
