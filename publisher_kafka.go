package main

import (
        "github.com/Shopify/sarama"
	"time"
)

func init() {
}

func PublishKafkav1(input chan []*FileEvent,
	registrar chan []*FileEvent,
	config *NetworkConfig) {
	var err error
	var sequence uint32
        pconfig := sarama.NewConfig()
	pconfig.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	pconfig.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
        producer, err := sarama.NewSyncProducer(config.Servers, pconfig)
	if err != nil {
		emit("Failed to start Sarama producer:", err)
                return;
	}

	for events := range input {

		for _, event := range events {
			sequence += 1
			//send here
			oops := func(err error) {
				// TODO(sissel): Track how frequently we timeout and reconnect. If we're
				// timing out too frequently, there's really no point in timing out since
				// basically everything is slow or down. We'll want to ratchet up the
				// timeout value slowly until things improve, then ratchet it down once
				// things seem healthy.
				emit("Socket error, will reconnect: %s\n", err)
				time.Sleep(1 * time.Second)
			}
			for {
		              partition, offset, err := producer.SendMessage(&sarama.ProducerMessage{
			          Topic: config.Topic,
			          Value: sarama.StringEncoder(*event.Text),
		              })
                              if err != nil {
                                  oops(err)
                                  continue
                              }
                              emit("Data sent to partition/offset: /%d/%d", partition, offset)
                              break
			}
		}

		// Tell the registrar that we've successfully sent these events
		registrar <- events
	}
}
