package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
)

const defaultStream = "stream"
const defaultSubject = "fruits"
const defaultConsumer = "stream1-consumer"
const defaultDeliverySubject = "subject1"

func main() {
	mode := flag.String("mode", "", "'publish' or 'consume'")
	natsUrl := flag.String("nats-url", "", "NATS server URL")
	recreate := flag.Bool("recreate", false, "recreate stream and consumers")
	flag.Parse()

	if *mode != "consume" && *mode != "publish" {
		flag.Usage()
		return
	}

	if *mode == "publish" {
		publish(*natsUrl, *recreate)
	} else if *mode == "consume" {
		consume(*natsUrl)
	}
}

func connectStream(natsUrl string) (*nats.Conn, nats.JetStreamContext, error) {
	if natsUrl == "" {
		natsUrl = nats.DefaultURL
	}

	nc, err := nats.Connect(natsUrl, nats.SyncQueueLen(1))
	if err != nil {
		return nil, nil, err
	}

	js, err := nc.JetStream()
	if err != nil {
		return nil, nil, err
	}

	return nc, js, nil
}

func publish(natsUrl string, recreate bool) error {
	_, js, err := connectStream(natsUrl)
	if err != nil {
		return fmt.Errorf("stream error: %s", err)
	}

	if recreate {
		js.DeleteStream(defaultStream)
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
			DeliverSubject: "delivery",
			DeliverGroup:   "common",
			AckPolicy:      nats.AckExplicitPolicy,
			MaxAckPending:  100,
			FilterSubject:  "fruits.tropical",
			FlowControl:    true,
			Heartbeat:      10 * time.Second,
		})
		js.AddConsumer(defaultStream, &nats.ConsumerConfig{
			Durable:        "berry",
			DeliverSubject: "delivery",
			DeliverGroup:   "common",
			AckPolicy:      nats.AckExplicitPolicy,
			MaxAckPending:  3,
			FilterSubject:  "fruits.berry",
			FlowControl:    true,
			Heartbeat:      10 * time.Second,
		})
	}

	for i := 1; i <= 1000; i++ {
		for _, subject := range []string{"fruits.tropical", "fruits.berry"} {

			_, err := js.Publish(subject, []byte(fmt.Sprintf("%s-%d", subject, i)), nats.MsgId(fmt.Sprintf("%d-%d", i, time.Now())))
			if err != nil {
				fmt.Printf("publisher error: %s\n", err.Error())
				return err
			}
			fmt.Printf("published to subject %s, id %d..\n", subject, i)
		}
	}

	return nil
}

func consume(natsUrl string) error {
	var counter int64

	nc, js, err := connectStream(natsUrl)
	_ = nc
	_ = js
	if err != nil {
		return fmt.Errorf("stream error: %s", err)
	}

	bandwidth := 10
	msgs := make(chan *nats.Msg, bandwidth)
	for i := 0; i < bandwidth; i++ {
		go func() {
			for {
				msg := <-msgs
				atomic.AddInt64(&counter, 1)
				if strings.Contains(msg.Subject, "fruits") {
					fmt.Printf("received message from (%s): %s\n", msg.Subject, string(msg.Data))
					time.Sleep(2 * time.Second)
				} else {
					reply := msg.Reply
					fmt.Printf("## received message from (%s): [%v] %s\n", msg.Header, reply, string(msg.Data))
				}

				msg.Ack()
				atomic.AddInt64(&counter, -1)
			}
		}()
	}

	tropicalSub, err := nc.QueueSubscribe("delivery", "common", func(msg *nats.Msg) {
		msgs <- msg
	})
	// tropicalSub, err := nc.QueueSubscribeSyncWithChan("delivery", "common", msgs)
	if err != nil {
		fmt.Printf("error queue subscrbe: %s\n", err)
		return err
	} else {
		tropicalSub.SetPendingLimits(1, -1)
	}

	go func() {
		for {
			fmt.Printf("- concurrent processor: %d\n", counter)
			x, _, err := tropicalSub.Pending()
			if err != nil {
				fmt.Printf("- pending message: %s\n", err)
			} else {
				fmt.Printf("- pending message: %d\n", x)
			}
			time.Sleep(2 * time.Second)
		}
	}()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT)
	<-sigs

	return nil
}
