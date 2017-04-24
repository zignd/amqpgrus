package amqpgrus

import (
	"fmt"

	"github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"
)

// QueueDeclaration represents settings used during the queue declaration.
type QueueDeclaration struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       amqp.Table
}

// Publishing represents settings used during the publising.
type Publishing struct {
	Exchange  string
	Key       string
	Mandatory bool
	Immediate bool
	Msg       amqp.Publishing
}

// Hook represents a Logrus hook.
type Hook struct {
	AMQPURL          string
	LoggingLevels    []logrus.Level
	QueueDeclaration QueueDeclaration
	Publishing       Publishing
}

// Levels returns the logging levels configured for this hook. The levels can be configured setting the LoggingLevels field.
func (h *Hook) Levels() []logrus.Level {
	if len(h.LoggingLevels) != 0 {
		return h.LoggingLevels
	}

	return []logrus.Level{
		logrus.PanicLevel,
		logrus.FatalLevel,
		logrus.ErrorLevel,
		logrus.WarnLevel,
		logrus.InfoLevel,
		logrus.DebugLevel,
	}
}

// Fire is called when logging on the logging levels returned from Levels.
func (h *Hook) Fire(entry *logrus.Entry) error {
	conn, err := amqp.Dial(h.AMQPURL)
	if err != nil {
		return fmt.Errorf("failed to create a new connection to %s: %v", h.AMQPURL, err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open a new server channel: %v", err)
	}
	defer ch.Close()

	_, err = ch.QueueDeclare(
		h.QueueDeclaration.Name,
		h.QueueDeclaration.Durable,
		h.QueueDeclaration.AutoDelete,
		h.QueueDeclaration.Exclusive,
		h.QueueDeclaration.NoWait,
		h.QueueDeclaration.Args,
	)
	if err != nil {
		return fmt.Errorf("failed to declare the %s queue: %v", h.QueueDeclaration.Name, err)
	}
	defer ch.Close()

	if h.Publishing.Msg.ContentType == "" {
		h.Publishing.Msg.ContentType = "text/plain"
	}

	body, err := entry.String()
	if err != nil {
		return err
	}

	h.Publishing.Msg.Body = []byte(body)

	err = ch.Publish(
		h.Publishing.Exchange,
		h.Publishing.Key,
		h.Publishing.Mandatory,
		h.Publishing.Immediate,
		h.Publishing.Msg,
	)
	if err != nil {
		return fmt.Errorf("failed to publish the entry %s: %v", body, err)
	}

	return nil
}

// NewHook creates a new Logrus Hook requesting the obligatory settings.
func NewHook(amqpURL, queueName string) *Hook {
	h := Hook{AMQPURL: amqpURL}
	h.QueueDeclaration.Name = queueName
	h.Publishing.Key = queueName
	return &h
}
