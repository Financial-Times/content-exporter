package queue

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/Financial-Times/content-exporter/content"
	"github.com/Financial-Times/content-exporter/export"
	"github.com/Financial-Times/kafka-client-go/v4"
)

const canBeDistributedYes = "yes"

// uuidRegexp enables to check if a string matches a UUID
var uuidRegexp = regexp.MustCompile("[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}")

type event struct {
	ContentURI string
	Payload    interface{}
}

func (e *event) toNotification(tid string) (*Notification, error) {
	uuid := uuidRegexp.FindString(e.ContentURI)
	if uuid == "" {
		return nil, fmt.Errorf("contentURI does not contain a UUID")
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
	canBeDistributedValue, ok := payload["canBeDistributed"]
	if ok {
		canBeDistributed = new(string)
		*canBeDistributed = canBeDistributedValue.(string)
	}

	contentType, _ := payload["type"].(string)

	var publication []string
	if _, ok = payload["publication"]; ok {
		publicationInterface, ok := payload["publication"].([]interface{})
		if !ok {
			return nil, fmt.Errorf("failed to convert publication to array")
		}

		for _, v := range publicationInterface {
			p, ok := v.(string)
			if !ok {
				return nil, fmt.Errorf("failed to convert publication to string")
			}
			publication = append(publication, p)
		}
	}

	return &Notification{
		Stub: content.Stub{
			UUID:             uuid,
			Date:             content.GetDateOrDefault(payload),
			CanBeDistributed: canBeDistributed,
			ContentType:      contentType,
			Publication:      publication,
		},
		EvType:     evType,
		Terminator: export.NewTerminator(),
		Tid:        tid,
	}, nil
}

type filterError struct {
	reason string
}

func newFilterError(reason string) error {
	return &filterError{
		reason: reason,
	}
}

func newFilterTypeError(contentType string) error {
	return &filterError{
		reason: fmt.Sprintf("type %s not allowed", contentType),
	}
}

func newFilterURIError(uri string) error {
	return &filterError{
		reason: fmt.Sprintf("uri %s not allowed", uri),
	}
}

func (e *filterError) Error() string {
	return fmt.Sprintf("content is not exportable: %s", e.reason)
}

type MessageMapper struct {
	originAllowlistRegex                     *regexp.Regexp
	allowedContentTypes, allowedPublishUUIDs map[string]bool
}

func NewMessageMapper(originAllowlist *regexp.Regexp, allowedContentTypes, allowedPublishUUIDs []string) *MessageMapper {
	allowedTypes := make(map[string]bool)
	for _, v := range allowedContentTypes {
		allowedTypes[v] = true
	}

	allowedUUIDs := make(map[string]bool)
	for _, v := range allowedPublishUUIDs {
		allowedUUIDs[v] = true
	}

	return &MessageMapper{
		originAllowlistRegex: originAllowlist,
		allowedContentTypes:  allowedTypes,
		allowedPublishUUIDs:  allowedUUIDs,
	}
}

func (m *MessageMapper) mapNotification(msg kafka.FTMessage) (*Notification, error) {
	tid := msg.Headers["X-Request-Id"]

	if strings.HasPrefix(tid, "SYNTH") {
		return nil, newFilterError("synthetic publication")
	}

	var pubEvent event
	if err := json.Unmarshal([]byte(msg.Body), &pubEvent); err != nil {
		return nil, fmt.Errorf("error unmarshaling event: %w", err)
	}

	if !m.originAllowlistRegex.MatchString(pubEvent.ContentURI) {
		return nil, newFilterURIError(pubEvent.ContentURI)
	}

	notification, err := pubEvent.toNotification(tid)
	if err != nil {
		return nil, fmt.Errorf("error building notification: %w", err)
	}

	if !m.allowedContentTypes[notification.Stub.ContentType] {
		return nil, newFilterTypeError(notification.Stub.ContentType)
	}

	if notification.Stub.Publication != nil {
		var present = false
		for _, v := range notification.Stub.Publication {
			if m.allowedPublishUUIDs[v] {
				present = true
			}
		}
		if !present {
			return nil, newFilterError("Unsupported publication")
		}
	}

	if notification.Stub.CanBeDistributed != nil && *notification.Stub.CanBeDistributed != canBeDistributedYes {
		return nil, newFilterError("cannot be distributed")
	}

	return notification, nil
}
