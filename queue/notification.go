package queue

import (
	"fmt"
	"time"

	"github.com/Financial-Times/content-exporter/content"
	"github.com/Financial-Times/content-exporter/export"
)

type EventType string

const (
	UPDATE EventType = "UPDATE"
	DELETE EventType = "DELETE"
)

type Notification struct {
	Stub   content.Stub
	EvType EventType
	Tid    string
	*export.Terminator
}

type NotificationHandler struct {
	exporter *content.Exporter
	delay    int
}

func NewNotificationHandler(exporter *content.Exporter, delayForNotification int) *NotificationHandler {
	return &NotificationHandler{
		exporter: exporter,
		delay:    delayForNotification,
	}
}

func (h *NotificationHandler) handleNotification(n *Notification) error {
	switch n.EvType {
	case UPDATE:
		select {
		case <-time.After(time.Duration(h.delay) * time.Second):
		case <-n.Quit:
			return fmt.Errorf("delayed update terminated due to shutdown signal")
		}

		if err := h.exporter.Export(n.Tid, &n.Stub); err != nil {
			return fmt.Errorf("exporting content: %w", err)
		}

	case DELETE:
		if err := h.exporter.Delete(n.Stub.UUID, n.Tid); err != nil {
			return fmt.Errorf("deleting content: %w", err)
		}

	default:
		return fmt.Errorf("unsupported event type")
	}

	return nil
}
