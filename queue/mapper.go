package queue

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/Financial-Times/content-exporter/content"
	"github.com/Financial-Times/content-exporter/export"
	"github.com/Financial-Times/kafka-client-go/kafka"
	log "github.com/sirupsen/logrus"
)

const canBeDistributedYes = "yes"

// UUIDRegexp enables to check if a string matches a UUID
var UUIDRegexp = regexp.MustCompile("[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}")

type MessageMapper interface {
	MapNotification(msg kafka.FTMessage) (*Notification, error)
}

type KafkaMessageMapper struct {
	WhiteListRegex *regexp.Regexp
}

func NewKafkaMessageMapper(whitelistR *regexp.Regexp) *KafkaMessageMapper {
	return &KafkaMessageMapper{WhiteListRegex: whitelistR}
}

type event struct {
	ContentURI string
	Payload    interface{}
}

func (e *event) mapNotification(tid string) (*Notification, error) {
	UUID := UUIDRegexp.FindString(e.ContentURI)
	if UUID == "" {
		return nil, fmt.Errorf("ContentURI does not contain a UUID")
	}

	payload, ok := e.Payload.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid payload type: %T", e.Payload)
	}

	evType := UPDATE
	if deleted, _ := payload["deleted"].(bool); deleted {
		evType = DELETE
	}

	var canBeDistributed *string
	canBeDistributedValue, found := payload["canBeDistributed"]
	if found {
		canBeDistributed = new(string)
		*canBeDistributed = canBeDistributedValue.(string)
	}

	return &Notification{
		Stub: content.Stub{
			Uuid:             UUID,
			Date:             content.GetDateOrDefault(payload),
			CanBeDistributed: canBeDistributed,
		},
		EvType:     evType,
		Terminator: export.NewTerminator(),
		Tid:        tid,
	}, nil
}

func (h *KafkaMessageMapper) MapNotification(msg kafka.FTMessage) (*Notification, error) {
	tid := msg.Headers["X-Request-Id"]
	var pubEvent event
	err := json.Unmarshal([]byte(msg.Body), &pubEvent)
	if err != nil {
		log.WithField("transaction_id", tid).WithField("msg", msg.Body).WithError(err).Warn("Skipping event.")
		return nil, err
	}

	if strings.HasPrefix(tid, "SYNTH") {
		log.WithField("transaction_id", tid).WithField("contentUri", pubEvent.ContentURI).Info("Skipping event: Synthetic transaction ID.")
		return nil, nil
	}

	if !h.WhiteListRegex.MatchString(pubEvent.ContentURI) {
		log.WithField("transaction_id", tid).WithField("contentUri", pubEvent.ContentURI).Info("Skipping event: It is not in the whitelist.")
		return nil, nil
	}

	n, err := pubEvent.mapNotification(tid)
	if err != nil {
		log.WithField("transaction_id", tid).WithField("msg", msg.Body).WithError(err).Warn("Skipping event: Cannot build notification for message.")
		return nil, err
	}

	if n.Stub.CanBeDistributed != nil && *n.Stub.CanBeDistributed != canBeDistributedYes {
		log.WithField("transaction_id", tid).WithField("uuid", n.Stub.Uuid).Warn("Skipping event: Content cannot be distributed.")
		return nil, nil
	}

	return n, nil
}
