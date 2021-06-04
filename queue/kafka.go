package queue

import (
	"errors"
	"sync"
	"time"

	"github.com/Financial-Times/content-exporter/export"
	"github.com/Financial-Times/kafka-client-go/kafka"
	log "github.com/sirupsen/logrus"
)

type MessageHandler interface {
	HandleMessage(queueMsg kafka.FTMessage) error
}

type KafkaListener struct {
	messageConsumer kafka.Consumer
	*export.Locker
	sync.RWMutex
	paused bool
	*export.Terminator
	received                   chan *Notification
	pending                    map[string]*Notification
	ContentNotificationHandler ContentNotificationHandler
	MessageMapper              MessageMapper
	worker                     chan struct{}
}

func NewKafkaListener(messageConsumer kafka.Consumer, notificationHandler *KafkaContentNotificationHandler, messageMapper *KafkaMessageMapper, locker *export.Locker, maxGoRoutines int) *KafkaListener {
	return &KafkaListener{
		messageConsumer:            messageConsumer,
		Locker:                     locker,
		received:                   make(chan *Notification, 1),
		pending:                    make(map[string]*Notification),
		Terminator:                 export.NewTerminator(),
		ContentNotificationHandler: notificationHandler,
		MessageMapper:              messageMapper,
		worker:                     make(chan struct{}, maxGoRoutines),
	}
}

func (h *KafkaListener) resumeConsuming() {
	h.Lock()
	defer h.Unlock()
	log.Debugf("DEBUG resumeConsuming")
	if h.paused {
		h.paused = false
	}
}

func (h *KafkaListener) pauseConsuming() {
	h.Lock()
	defer h.Unlock()
	log.Debugf("DEBUG pauseConsuming")
	if !h.paused {
		h.paused = true
	}
}

func (h *KafkaListener) ConsumeMessages() {
	h.messageConsumer.StartListening(h.HandleMessage)
	go h.handleNotifications()

	defer func() {
		h.Terminator.ShutDownPrepared = true
		h.TerminatePendingNotifications()
	}()
	defer h.messageConsumer.Shutdown()

	for {
		select {
		case locked := <-h.Locker.Locked:
			log.Infof("LOCK signal received: %v...", locked)
			if locked {
				h.pauseConsuming()
				select {
				case h.Locker.Acked <- struct{}{}:
					log.Infof("LOCK acked")
				case <-time.After(time.Second * 3):
					log.Infof("LOCK acking timed out. Maybe initiator quit already?")
				}
			} else {
				h.resumeConsuming()
			}
		case <-h.Quit:
			log.Infof("QUIT signal received...")
			return
		}
	}
}

func (h *KafkaListener) StopConsumingMessages() {
	h.Quit <- struct{}{}
	for {
		if h.ShutDown {
			return
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func (h *KafkaListener) HandleMessage(msg kafka.FTMessage) error {
	if h.ShutDownPrepared {
		h.Cleanup.Do(func() {
			close(h.received)
		})
		return errors.New("Service is shutdown")
	}

	tid := msg.Headers["X-Request-Id"]

	if h.paused {
		log.WithField("transaction_id", tid).Info("PAUSED handling message")
		for h.paused {
			if h.ShutDownPrepared {
				h.Cleanup.Do(func() {
					close(h.received)
				})
				return errors.New("Service is shutdown")
			}
			time.Sleep(time.Millisecond * 500)
		}
		log.WithField("transaction_id", tid).Info("PAUSE finished. Resuming handling messages")
	}

	n, err := h.MessageMapper.MapNotification(msg)
	if n == nil {
		return err
	}
	h.Lock()
	h.pending[n.Tid] = n
	h.Unlock()
	select {
	case h.received <- n:
	case <-n.Quit:
		log.WithField("transaction_id", tid).WithField("uuid", n.Stub.Uuid).Error("Notification handling is terminated")
		return errors.New("Notification handling is terminated")

	}
	if h.ShutDownPrepared {
		h.Cleanup.Do(func() {
			close(h.received)
		})
	}
	return err
}

func (h *KafkaListener) handleNotifications() {
	log.Info("Started handling notifications")
	for n := range h.received {
		if h.paused {
			log.WithField("transaction_id", n.Tid).Info("PAUSED handling notification")
			for h.paused {
				time.Sleep(time.Millisecond * 500)
			}
			log.WithField("transaction_id", n.Tid).Info("PAUSE finished. Resuming handling notification")
		}
		h.worker <- struct{}{}
		go func(notification *Notification) {
			defer func() { <-h.worker }()
			if err := h.ContentNotificationHandler.HandleContentNotification(notification); err != nil {
				log.WithField("transaction_id", notification.Tid).WithField("uuid", notification.Stub.Uuid).WithError(err).Error("Failed notification handling")
			}
			h.Lock()
			delete(h.pending, notification.Tid)
			h.Unlock()
		}(n)
	}
	log.Info("Stopped handling notifications")
	h.ShutDown = true
}

func (h *KafkaListener) TerminatePendingNotifications() {
	h.RLock()
	for _, n := range h.pending {
		n.Quit <- struct{}{}
		close(n.Quit)
	}
	h.RUnlock()
}

func (h *KafkaListener) CheckHealth() (string, error) {
	if err := h.messageConsumer.ConnectivityCheck(); err != nil {
		return "Kafka is not good to go.", err
	}
	return "Kafka is good to go.", nil
}
