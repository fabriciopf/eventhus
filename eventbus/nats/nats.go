package nats

import (
	"encoding/json"
	"strings"

	"github.com/fabriciopf/eventhus"
	nats "github.com/nats-io/go-nats"
)

// Client nats
type Client struct {
	Options nats.Options
}

// NewClient returns the basic client to access to nats
func NewClient(urls string, useTLS bool) (*Client, error) {
	opts := nats.DefaultOptions
	opts.Secure = useTLS
	opts.Servers = strings.Split(urls, ",")

	for i, s := range opts.Servers {
		opts.Servers[i] = strings.Trim(s, " ")
	}

	return &Client{
		opts,
	}, nil
}

// Publish a event
func (c *Client) Publish(event eventhus.Event, bucket, subset string) error {
	nc, err := c.Options.Connect()
	if err != nil {
		return err
	}

	defer nc.Close()

	blob, err := json.Marshal(event)
	if err != nil {
		return err
	}

	subj := bucket + "." + subset + "." + event.Type + "." + event.AggregateID
	nc.Publish(subj, blob)
	nc.Flush()

	err = nc.LastError()
	return err
}

func (c *Client) Subscribe(pattern string, cb func(event eventhus.Event)) error {
	nc, err := c.Options.Connect()
	if err != nil {
		return err
	}

	defer nc.Close()

	nc.Subscribe(pattern, func(msg *nats.Msg) {
		var event eventhus.Event
		tokens := strings.Split(msg.Subject, ".")
		event.Type = tokens[2]
		event.AggregateType = tokens[1]
		event.AggregateID = tokens[3]
		event.Data = msg.Data
		msg.Sub.Unsubscribe()
		go cb(event)
	})

	nc.Flush()
	err = nc.LastError()
	return err

}
