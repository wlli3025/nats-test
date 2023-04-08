package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	workerStream   = "w"
	workerConsumer = "wc"
	jobStream      = "j"
	jobConsumer    = "jc"
)

var jobToWorkerCapacity map[string]int

func main() {
	mode := flag.String("mode", "", "'publish' or 'consume'")
	natsUrl := flag.String("nats-url", "", "NATS server URL")
	recreate := flag.Bool("recreate", false, "recreate stream and consumers")
	flag.Parse()

	rand.Seed(time.Now().UnixNano())

	jobToWorkerCapacity = map[string]int{
		"build": 1,
		"test":  1,
		// "deploy":  6,
		// "execute": 8,
	}

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

func publish(natsUrl string, recreate bool) error {
	_, js, err := connectStream(natsUrl)
	if err != nil {
		return fmt.Errorf("stream error: %s", err)
	}

	if recreate {
		// create job queue
		js.DeleteStream(jobStream)
		js.AddStream(&nats.StreamConfig{
			Name:     jobStream,
			Subjects: []string{"job.>"},
		})
		js.DeleteConsumer(jobStream, jobConsumer)
		js.AddConsumer(jobStream, &nats.ConsumerConfig{
			Durable:   jobConsumer,
			AckPolicy: nats.AckExplicitPolicy,
			// retry job after 30 sec
			AckWait: 30 * time.Second,
			// discard after 10 attempts
			MaxDeliver: 10,
			// allow many pull subscribers
			MaxWaiting: 65535,
		})

		// create worker consumer queues to manage maximum number of workers for each job type
		for jobType, num := range jobToWorkerCapacity {
			streamName := fmt.Sprintf("%s-%s", workerStream, jobType)
			streamSubject := fmt.Sprintf("%s.%s", workerStream, jobType)
			js.DeleteStream(streamName)
			js.AddStream(&nats.StreamConfig{
				Name:     streamName,
				Subjects: []string{streamSubject},
			})
			if err != nil {
				fmt.Printf("add stream error: %s\n", err.Error())
				return err
			}

			consumerName := fmt.Sprintf("%s-%s", workerConsumer, jobType)
			js.DeleteConsumer(streamName, consumerName)
			js.AddConsumer(streamName, &nats.ConsumerConfig{
				Durable:   consumerName,
				AckPolicy: nats.AckExplicitPolicy,
				// shorter wait time here to return worker if any pull consumer crash
				AckWait: 10 * time.Second,
				// max concurrent workers allowed to consume from job queue for a particular job type
				// regardless how many workers are available for that job type
				MaxAckPending: 100,
				// allow many pull subscribers
				MaxWaiting: 65535,
			})

			// pepare available workers for each job type
			for i := 0; i < num; i++ {
				js.PublishAsync(streamSubject, []byte(fmt.Sprintf("%s-%d", jobType, i)))
			}
		}
	}

	// publish the jobs to job stream
	totalMsgPerJob := 1000
	i := 1
	for i <= totalMsgPerJob {
		for jobType := range jobToWorkerCapacity {
			var err error
			subject := getSubjectFromJobType(jobType)
			if i != totalMsgPerJob {
				_, err = js.PublishAsync(subject, []byte(fmt.Sprintf("%s-%d", subject, i)), nats.MsgId(fmt.Sprintf("%d-%d", i, time.Now())))
			} else {
				_, err = js.Publish(subject, []byte(fmt.Sprintf("%s-%d", subject, i)), nats.MsgId(fmt.Sprintf("%d-%d", i, time.Now())))
			}
			if err != nil {
				fmt.Printf("publisher error: %s\n", err.Error())
				return err
			}
			fmt.Printf("published to subject %s, id %d..\n", subject, i)
			i++
		}
	}

	return nil
}

func consume(natsUrl string) error {
	_, js, err := connectStream(natsUrl)
	if err != nil {
		return fmt.Errorf("stream error: %s", err)
	}

	// subscribe to job consumer
	jobSub, err := js.PullSubscribe("", jobConsumer, nats.Bind(jobStream, jobConsumer))
	if err != nil {
		fmt.Printf("error subscribe pull: %s\n", err)
		return err
	}

	// subscribe to worker streams
	workers := make(map[string]*nats.Subscription)
	for jobType := range jobToWorkerCapacity {
		streamName := fmt.Sprintf("%s-%s", workerStream, jobType)
		consumerName := fmt.Sprintf("%s-%s", workerConsumer, jobType)
		if sub, err := js.PullSubscribe("", consumerName, nats.Bind(streamName, consumerName)); err != nil {
			fmt.Printf("error subscribe pull: %s\n", err)
			return err
		} else {
			workers[jobType] = sub
		}
	}

	go func() {
		releaseWorker := make(chan bool)
		for {
			results, err := jobSub.Fetch(1, nats.MaxWait(1*time.Second))
			if err != nil {
				fmt.Printf("error pull message: %s\n", err)
			} else {
				msg := results[0]
				fmt.Printf("received message from (%s): %s\n", msg.Subject, string(msg.Data))

				// look for an available worker for this job type
				jobType := getJobTypeFromSubject(msg.Subject)
				workerSlots, err := workers[jobType].Fetch(1, nats.MaxWait(2*time.Second))
				if err != nil {
					fmt.Printf("* error acquire [%s] worker: %s\n", jobType, err)

					// no job ack or nack here, let redeliver happen after AckWait timeout
				} else {
					fmt.Printf("acquired worker for %s\n", jobType)

					// acquired a free worker
					// process the job
					tick := time.NewTicker(5 * time.Second)
					go func() {
						for {
							select {
							case <-releaseWorker:
								return
							case <-tick.C:
								workerSlots[0].InProgress()
							}
						}
					}()

					time.Sleep(time.Duration((rand.Intn(5) + 1)) * time.Second)
					msg.AckSync()

					// return the worker back to queue
					// subject := fmt.Sprintf("%s.%s", workerStream, jobType)
					// js.Publish(subject, w[0].Data)
					tick.Stop()
					releaseWorker <- true
					workerSlots[0].Nak()
				}
			}
		}
	}()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT)
	<-sigs

	return nil
}

func getJobTypeFromSubject(subject string) string {
	return strings.Split(subject, ".")[1]
}

func getSubjectFromJobType(jobType string) string {
	return fmt.Sprintf("%s.%s", "job", jobType)
}
