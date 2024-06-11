package queue

import (
	"sync"
	"time"

	"github.com/Financial-Times/content-exporter/export"
	"github.com/Financial-Times/content-exporter/policy"
	"github.com/Financial-Times/go-logger/v2"
	"github.com/Financial-Times/kafka-client-go/v4"
)

type Agent interface {
	EvaluateContentPolicy(q map[string]interface{}) (*policy.ContentPolicyResult, error)
}

type Listener struct {
	messageConsumer *kafka.Consumer
	locker          *export.Locker
	sync.RWMutex
	paused              bool
	terminator          *export.Terminator
	received            chan *Notification
	pending             map[string]*Notification
	notificationHandler *NotificationHandler
	messageMapper       *MessageMapper
	policyEvaluator     Agent
	workers             chan struct{}
	log                 *logger.UPPLogger
}

func NewListener(
	messageConsumer *kafka.Consumer,
	notificationHandler *NotificationHandler,
	messageMapper *MessageMapper,
	policyEvaluator Agent,
	locker *export.Locker,
	maxGoRoutines int,
	log *logger.UPPLogger,
) *Listener {
	return &Listener{
		messageConsumer:     messageConsumer,
		locker:              locker,
		received:            make(chan *Notification, 1),
		pending:             make(map[string]*Notification),
		terminator:          export.NewTerminator(),
		notificationHandler: notificationHandler,
		messageMapper:       messageMapper,
		policyEvaluator:     policyEvaluator,
		workers:             make(chan struct{}, maxGoRoutines),
		log:                 log,
	}
}

func (l *Listener) resumeConsuming() {
	l.Lock()
	defer l.Unlock()
	l.log.Debug("Resuming message consumer")
	l.paused = false
}

func (l *Listener) pauseConsuming() {
	l.Lock()
	defer l.Unlock()
	l.log.Debug("Pausing message consumer")
	l.paused = true
}

func (l *Listener) Start() {
	l.messageConsumer.Start(l.handleMessage)
	go l.handleNotifications()

	defer func() {
		l.terminator.ShutDownPrepared = true
		l.terminatePendingNotifications()
	}()

	for {
		select {
		case locked := <-l.locker.Locked:
			l.log.Infof("LOCK signal received: %v...", locked)
			if locked {
				l.pauseConsuming()
				select {
				case l.locker.Acked <- struct{}{}:
					l.log.Info("LOCK acked")
				case <-time.After(time.Second * 3):
					l.log.Info("LOCK acking timed out. Maybe initiator quit already?")
				}
			} else {
				l.resumeConsuming()
			}
		case <-l.terminator.Quit:
			l.log.Info("QUIT signal received...")
			return
		}
	}
}

func (l *Listener) Stop() {
	l.terminator.Quit <- struct{}{}

	if err := l.messageConsumer.Close(); err != nil {
		l.log.WithError(err).Error("Error closing consumer")
	}

	for {
		if l.terminator.ShutDown {
			return
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func (l *Listener) cleanup() {
	l.terminator.Cleanup.Do(func() {
		close(l.received)
	})
}

func (l *Listener) handleMessage(msg kafka.FTMessage) {
	if l.terminator.ShutDownPrepared {
		l.cleanup()
		return
	}

	tid := msg.Headers["X-Request-Id"]
	log := l.log.WithTransactionID(tid)

	if l.paused {
		log.Info("PAUSED handling message")
		for l.paused {
			if l.terminator.ShutDownPrepared {
				l.cleanup()
				return
			}
			time.Sleep(time.Millisecond * 500)
		}
		log.Info("PAUSE finished. Resuming handling messages")
	}

	n, err := l.messageMapper.mapNotification(msg)
	if err != nil {
		log = log.WithError(err)
		message := "Skipping event"

		if _, ok := err.(*filterError); ok {
			log.Info(message)
		} else {
			log.Warn(message)
		}
		return
	}

	input := map[string]interface{}{
		"payload": map[string]interface{}{
			"publication": n.Stub.Publication,
		},
	}

	res, err := l.policyEvaluator.EvaluateContentPolicy(input)
	if err != nil {
		log.WithError(err).Error("Error with policy evaluation")
		return
	}
	if res.Skip {
		log.WithField("reasons", res.Reasons).Info("Skipping SV content")
		return
	}

	l.Lock()
	l.pending[n.Tid] = n
	l.Unlock()
	select {
	case l.received <- n:
	case <-n.Quit:
		log.WithUUID(n.Stub.UUID).Error("Notification handling is terminated")
		return
	}
	if l.terminator.ShutDownPrepared {
		l.cleanup()
	}
}

func (l *Listener) handleNotifications() {
	l.log.Info("Started handling notifications")
	for n := range l.received {
		log := l.log.WithTransactionID(n.Tid)

		if l.paused {
			log.Info("PAUSED handling notification")
			for l.paused {
				time.Sleep(time.Millisecond * 500)
			}
			log.Info("PAUSE finished. Resuming handling notification")
		}

		l.workers <- struct{}{}
		go func(notification *Notification, log *logger.LogEntry) {
			defer func() {
				<-l.workers
			}()

			log = log.
				WithUUID(notification.Stub.UUID).
				WithField("event_type", notification.EvType)

			err := l.notificationHandler.handleNotification(notification)
			if err != nil {
				log.WithError(err).Error("Failed to handle notification")
			} else {
				log.Info("Successfully handled notification")
			}

			l.Lock()
			delete(l.pending, notification.Tid)
			l.Unlock()
		}(n, log)
	}
	l.log.Info("Stopped handling notifications")
	l.terminator.ShutDown = true
}

func (l *Listener) terminatePendingNotifications() {
	l.RLock()
	defer l.RUnlock()

	for _, n := range l.pending {
		n.Quit <- struct{}{}
		close(n.Quit)
	}
}

func (l *Listener) CheckHealth() (string, error) {
	if err := l.messageConsumer.ConnectivityCheck(); err != nil {
		return "", err
	}
	return "Connectivity to Kafka is OK", nil
}

func (l *Listener) MonitorCheck() (string, error) {
	if err := l.messageConsumer.MonitorCheck(); err != nil {
		return "", err
	}
	return "Kafka consumer status is OK", nil
}
