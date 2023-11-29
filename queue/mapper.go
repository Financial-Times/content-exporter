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

type payload struct {
	Deleted            bool     `json:"deleted,omitempty"`
	CanBeDistributed   string   `json:"canBeDistributed,omitempty"`
	Type               string   `json:"type"`
	Publication        []string `json:"publication"`
	FirstPublishedDate string   `json:"firstPublishedDate,omitempty"`
	PublishedDate      string   `json:"publishedDate,omitempty"`
}

func (p payload) getDateOrDefault() string {
	if p.FirstPublishedDate != "" {
		if date := strings.Split(p.FirstPublishedDate, "T")[0]; date != "" {
			return date
		}
	}

	if p.PublishedDate != "" {
		if date := strings.Split(p.PublishedDate, "T")[0]; date != "" {
			return date
		}
	}

	return content.DefaultDate
}

type event struct {
	ContentURI string
	Payload    payload
}

func (e *event) toNotification(tid string) (*Notification, error) {
	uuid := uuidRegexp.FindString(e.ContentURI)
	if uuid == "" {
		return nil, fmt.Errorf("contentURI does not contain a UUID")
	}

	evType := UPDATE
	if e.Payload.Deleted {
		evType = DELETE
	}

	return &Notification{
		Stub: content.Stub{
			UUID:             uuid,
			Date:             e.Payload.getDateOrDefault(),
			CanBeDistributed: e.Payload.CanBeDistributed,
			ContentType:      e.Payload.Type,
			Publication:      e.Payload.Publication,
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

	if notification.Stub.CanBeDistributed != "" && notification.Stub.CanBeDistributed != canBeDistributedYes {
		return nil, newFilterError("cannot be distributed")
	}

	return notification, nil
}
