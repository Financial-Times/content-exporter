package queue

import (
	"sync"
	"time"

	"github.com/Financial-Times/content-exporter/export"
	"github.com/Financial-Times/go-logger/v2"
	"github.com/Financial-Times/kafka-client-go/v3"
)

type KafkaListener struct {
	messageConsumer *kafka.Consumer
	*export.Locker
	sync.RWMutex
	paused bool
	*export.Terminator
	received                   chan *Notification
	pending                    map[string]*Notification
	ContentNotificationHandler ContentNotificationHandler
	MessageMapper              MessageMapper
	worker                     chan struct{}
	log                        *logger.UPPLogger
}

func NewKafkaListener(
	messageConsumer *kafka.Consumer,
	notificationHandler *KafkaContentNotificationHandler,
	messageMapper *KafkaMessageMapper,
	locker *export.Locker,
	maxGoRoutines int,
	log *logger.UPPLogger,
) *KafkaListener {
	return &KafkaListener{
		messageConsumer:            messageConsumer,
		Locker:                     locker,
		received:                   make(chan *Notification, 1),
		pending:                    make(map[string]*Notification),
		Terminator:                 export.NewTerminator(),
		ContentNotificationHandler: notificationHandler,
		MessageMapper:              messageMapper,
		worker:                     make(chan struct{}, maxGoRoutines),
		log:                        log,
	}
}

func (h *KafkaListener) resumeConsuming() {
	h.Lock()
	defer h.Unlock()
	h.log.Debugf("DEBUG resumeConsuming")
	h.paused = false
}

func (h *KafkaListener) pauseConsuming() {
	h.Lock()
	defer h.Unlock()
	h.log.Debugf("DEBUG pauseConsuming")
	h.paused = true
}

func (h *KafkaListener) ConsumeMessages() {
	h.messageConsumer.Start(h.HandleMessage)
	go h.handleNotifications()

	defer func() {
		h.Terminator.ShutDownPrepared = true
		h.TerminatePendingNotifications()
	}()
	defer func(messageConsumer *kafka.Consumer) {
		err := messageConsumer.Close()
		if err != nil {
			h.log.WithError(err).Error("Consumer did not close properly")
		}
	}(h.messageConsumer)

	for {
		select {
		case locked := <-h.Locker.Locked:
			h.log.Infof("LOCK signal received: %v...", locked)
			if locked {
				h.pauseConsuming()
				select {
				case h.Locker.Acked <- struct{}{}:
					h.log.Infof("LOCK acked")
				case <-time.After(time.Second * 3):
					h.log.Infof("LOCK acking timed out. Maybe initiator quit already?")
				}
			} else {
				h.resumeConsuming()
			}
		case <-h.Quit:
			h.log.Infof("QUIT signal received...")
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

func (h *KafkaListener) HandleMessage(msg kafka.FTMessage) {
	if h.ShutDownPrepared {
		h.Cleanup.Do(func() {
			close(h.received)
		})
		return
	}

	tid := msg.Headers["X-Request-Id"]
	log := h.log.WithTransactionID(tid)

	if h.paused {
		log.Info("PAUSED handling message")
		for h.paused {
			if h.ShutDownPrepared {
				h.Cleanup.Do(func() {
					close(h.received)
				})
				return
			}
			time.Sleep(time.Millisecond * 500)
		}
		log.Info("PAUSE finished. Resuming handling messages")
	}

	n, _ := h.MessageMapper.MapNotification(msg)
	if n == nil {
		return
	}
	h.Lock()
	h.pending[n.Tid] = n
	h.Unlock()
	select {
	case h.received <- n:
	case <-n.Quit:
		log.WithUUID(n.Stub.UUID).Error("Notification handling is terminated")
		return
	}
	if h.ShutDownPrepared {
		h.Cleanup.Do(func() {
			close(h.received)
		})
	}
}

func (h *KafkaListener) handleNotifications() {
	h.log.Info("Started handling notifications")
	for n := range h.received {
		log := h.log.WithTransactionID(n.Tid)

		if h.paused {
			log.Info("PAUSED handling notification")
			for h.paused {
				time.Sleep(time.Millisecond * 500)
			}
			log.Info("PAUSE finished. Resuming handling notification")
		}
		h.worker <- struct{}{}
		go func(notification *Notification) {
			defer func() { <-h.worker }()
			if err := h.ContentNotificationHandler.HandleContentNotification(notification); err != nil {
				log.WithUUID(notification.Stub.UUID).WithError(err).Error("Failed notification handling")
			}
			h.Lock()
			delete(h.pending, notification.Tid)
			h.Unlock()
		}(n)
	}
	h.log.Info("Stopped handling notifications")
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

func (h *KafkaListener) MonitorCheck() (string, error) {
	if err := h.messageConsumer.MonitorCheck(); err != nil {
		return "Kafka is lagging.", err
	}
	return "Kafka is good to go.", nil
}
