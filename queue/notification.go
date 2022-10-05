package queue

import (
	"fmt"
	"time"

	"github.com/Financial-Times/content-exporter/content"
	"github.com/Financial-Times/content-exporter/export"
	"github.com/Financial-Times/go-logger/v2"
)

type EventType string

const UPDATE EventType = "UPDATE"
const DELETE EventType = "DELETE"

type Notification struct {
	Stub   content.Stub
	EvType EventType
	Tid    string
	*export.Terminator
}

type ContentNotificationHandler interface {
	HandleContentNotification(n *Notification) error
}

type KafkaContentNotificationHandler struct {
	ContentExporter *content.Exporter
	Delay           int
	log             *logger.UPPLogger
}

func NewKafkaContentNotificationHandler(exporter *content.Exporter, delayForNotification int, log *logger.UPPLogger) *KafkaContentNotificationHandler {
	return &KafkaContentNotificationHandler{
		ContentExporter: exporter,
		Delay:           delayForNotification,
		log:             log,
	}
}

func (h *KafkaContentNotificationHandler) HandleContentNotification(n *Notification) error {
	logEntry := h.log.WithTransactionID(n.Tid).WithUUID(n.Stub.UUID)
	if n.EvType == UPDATE {
		logEntry.Infof("UPDATE event received. Waiting configured delay - %v second(s)", h.Delay)

		select {
		case <-time.After(time.Duration(h.Delay) * time.Second):
		case <-n.Quit:
			err := fmt.Errorf("shutdown signalled, delay waiting for UPDATE event terminated abruptly")
			return err
		}
		if err := h.ContentExporter.Export(n.Tid, &n.Stub); err != nil {
			return fmt.Errorf("UPDATE ERROR: %v", err)
		}
	} else if n.EvType == DELETE {
		logEntry.Info("DELETE event received")
		if err := h.ContentExporter.Delete(n.Stub.UUID, n.Tid); err != nil {
			if err == content.ErrNotFound {
				logEntry.Warn(err)
				return nil
			}
			return fmt.Errorf("DELETE ERROR: %v", err)
		}
	}
	return nil
}
