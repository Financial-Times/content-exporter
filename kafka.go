package main

import (
	"encoding/json"
	"fmt"
	"github.com/Financial-Times/kafka-client-go/kafka"
	log "github.com/sirupsen/logrus"
	"regexp"
	"strings"
	"sync"
	"time"
)

type eventType string

const UPDATE eventType = "UPDATE"
const DELETE eventType = "DELETE"

type MessageQueueHandler interface {
	HandleMessage(queueMsg kafka.FTMessage) error
}

type KafkaMessageHandler struct {
	messageConsumer kafka.Consumer
	ContentExporter *ContentExporter
	Delay           int
	WhiteListRegex  *regexp.Regexp
	*Locker
	sync.RWMutex
	running         bool
	notifCh         chan Notification
}

func NewKafkaMessageHandler(exporter *ContentExporter, delayForNotification int, messageConsumer kafka.Consumer, whitelistR *regexp.Regexp, locker *Locker) *KafkaMessageHandler {
	return &KafkaMessageHandler{
		ContentExporter: exporter,
		Delay:           delayForNotification,
		messageConsumer: messageConsumer,
		WhiteListRegex:  whitelistR,
		Locker:          locker,
		notifCh: make(chan Notification, 30), //TODO when to close? is 30 as a buffer ok?
	}
}

type NotificationQueueMessage struct {
	kafka.FTMessage
}

type PublicationEvent struct {
	ContentURI   string
	UUID         string
	Payload      interface{}
	LastModified string
}

func (e PublicationEvent) HasEmptyPayload() bool {
	switch v := e.Payload.(type) {
	case nil:
		return true
	case string:
		if len(v) == 0 {
			return true
		}
	case map[string]interface{}:
		if len(v) == 0 {
			return true
		}
	}
	return false
}

func (e PublicationEvent) Matches(whiteList *regexp.Regexp) bool {
	return whiteList.MatchString(e.ContentURI)
}

func (msg NotificationQueueMessage) ToPublicationEvent() (event PublicationEvent, err error) {
	err = json.Unmarshal([]byte(msg.Body), &event)
	return event, err
}

func (msg NotificationQueueMessage) HasSynthTransactionID() bool {
	tid := msg.TransactionID()
	return strings.HasPrefix(tid, "SYNTH")
}

func (msg NotificationQueueMessage) TransactionID() string {
	return msg.Headers["X-Request-Id"]
}

func (h *KafkaMessageHandler) startConsuming() {
	h.Lock()
	defer h.Unlock()
	log.Infof("DEBUG startConsuming")
	if !h.running {
		h.running = true
		//h.messageConsumer.StartListening(h.handleMessage)
		log.Infof("DEBUG StartListening called")
	}
}

func (h *KafkaMessageHandler) stopConsuming() {
	h.Lock()
	defer h.Unlock()
	log.Infof("DEBUG stopConsuming")
	if h.running {
		h.running = false
		//h.messageConsumer.Shutdown()
		log.Info("DEBUG Shutdown called")
	}
}

func (h *KafkaMessageHandler) ConsumeMessages() {
	h.messageConsumer.StartListening(h.handleMessage)
	go h.handleNotification()
	h.startConsuming()
	defer h.stopConsuming()
	defer close(h.notifCh)
	defer h.messageConsumer.Shutdown()
	for {
		select {
		case locked := <-h.Locker.locked:
			log.Infof("LOCK signal received: %v...", locked)
			if locked {
				h.stopConsuming()
				select {
				case h.Locker.acked <- struct{}{}:
					log.Infof("LOCK acked")
				case <-time.After(time.Second * 3):
					log.Infof("LOCK acking timed out. Maybe initiator quit already?")
				}
			} else {
				h.startConsuming()
			}
		case <-h.quit:
			log.Infof("QUIT signal received...")

			return
		}
	}
}

func (h *KafkaMessageHandler) StopConsumingMessages() {
	h.quit <- struct{}{}
	for {
		if !h.running {
			return
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func (h *KafkaMessageHandler) handleMessage(queueMsg kafka.FTMessage) error {
	msg := NotificationQueueMessage{queueMsg}

	pubEvent, err := msg.ToPublicationEvent()
	tid := msg.TransactionID()
	if !h.running {
		log.WithField("transaction_id", tid).Info("PAUSED handling message")
		for !h.running {
			time.Sleep(time.Millisecond * 500)
		}
		log.WithField("transaction_id", tid).Info("PAUSE finished. Resuming handling messages")
	}
	if err != nil {
		log.WithField("transaction_id", tid).WithField("msg", msg.Body).WithError(err).Warn("Skipping event.")
		return err
	}

	if msg.HasSynthTransactionID() {
		log.WithField("transaction_id", tid).WithField("contentUri", pubEvent.ContentURI).Info("Skipping event: Synthetic transaction ID.")
		return nil
	}

	if !pubEvent.Matches(h.WhiteListRegex) {
		log.WithField("transaction_id", tid).WithField("contentUri", pubEvent.ContentURI).Info("Skipping event: It is not in the whitelist.")
		return nil
	}

	notif, err := h.MapNotification(pubEvent)
	if err != nil {
		log.WithField("transaction_id", tid).WithField("msg", msg.Body).WithError(err).Warn("Skipping event: Cannot build notification for message.")
		return err
	}
	notif.tid = tid
	h.notifCh <- notif
	return nil
}

func (h *KafkaMessageHandler) handleNotification() {
	log.Info("Started handling notifications")
	for notif := range h.notifCh {
		if !h.running {
			log.WithField("transaction_id", notif.tid).Info("PAUSED handling notification")
			for !h.running {
				time.Sleep(time.Millisecond * 500)
			}
			log.WithField("transaction_id", notif.tid).Info("PAUSE finished. Resuming handling notification")
		}
		logEntry := log.WithField("transaction_id", notif.tid).WithField("uuid", notif.content.Uuid)
		if notif.evType == UPDATE {
			logEntry.Infof("UPDATE event received. Waiting configured delay - %v second(s)", h.Delay)
			time.Sleep(time.Duration(h.Delay) * time.Second)
			if err := h.ContentExporter.HandleContent(notif.tid, notif.content); err != nil {
				log.WithField("transaction_id", notif.tid).WithField("uuid", notif.content.Uuid).WithError(err).Error("FAILED UPDATE event")
			}
		} else if notif.evType == DELETE {
			logEntry.Info("DELETE event received")
			if err := h.ContentExporter.Uploader.Delete(notif.content.Uuid, notif.tid); err != nil {
				logEntry.WithError(err).Error("FAILED DELETE event")
			}
		}

	}
	log.Info("Stopped handling notifications")
}

func (h *KafkaMessageHandler) CheckHealth() (string, error) {
	if err := h.messageConsumer.ConnectivityCheck(); err != nil {
		return "Kafka is not good to go.", err
	}
	return "Kafka is good to go.", nil
}

// UUIDRegexp enables to check if a string matches a UUID
var UUIDRegexp = regexp.MustCompile("[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}")

type Notification struct {
	content Content
	evType eventType
	tid string
}

// MapNotification maps the given event to a new notification.
func (h *KafkaMessageHandler) MapNotification(event PublicationEvent) (Notification, error) {
	UUID := UUIDRegexp.FindString(event.ContentURI)
	if UUID == "" {
		return Notification{content: Content{}, evType: eventType("")}, fmt.Errorf("ContentURI does not contain a UUID")
	}

	var evType eventType
	var date = defaultDate

	if event.HasEmptyPayload() {
		evType = DELETE
	} else {
		evType = UPDATE
		notificationPayloadMap, ok := event.Payload.(map[string]interface{})
		if ok {
			date = getDate(notificationPayloadMap)
		}
	}

	return Notification{content: Content{
		Uuid: UUID,
		Date: date,
	}, evType: evType}, nil
}
