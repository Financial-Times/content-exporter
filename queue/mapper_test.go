package queue

import (
	"encoding/json"
	"regexp"
	"testing"

	"github.com/Financial-Times/content-exporter/content"
	"github.com/Financial-Times/kafka-client-go/v3"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func generateRequestBody(contentURI string, payload interface{}) string {
	body, err := json.Marshal(event{
		ContentURI: contentURI,
		Payload:    payload,
	})
	if err != nil {
		return ""
	}
	return string(body)
}

func TestKafkaMessageMapper_MapNotification(t *testing.T) {
	tests := []struct {
		name                        string
		contentOriginAllowlistRegex *regexp.Regexp
		allowedContentTypes         []string
		msg                         kafka.FTMessage
		expectedNotification        *Notification
		error                       string
	}{
		{
			name:                        "synthetic transaction ID will skip mapping",
			contentOriginAllowlistRegex: regexp.MustCompile(`^http://(wordpress|upp)-(article|content)-(transformer|mapper|validator)(-pr|-iw)?(-uk-.*)?\.svc\.ft\.com(:\d{2,5})?/(content|audio)/[\w-]+.*$`),
			allowedContentTypes:         []string{"Audio", "Article", "LiveBlogPost", "LiveBlogPackage", "Content"},
			msg: kafka.FTMessage{
				Headers: map[string]string{"X-Request-Id": "SYNTH_REQ_MON1"},
				Body:    generateRequestBody("http://upp-content-validator.svc.ft.com/content/811e0591-5c71-4457-b8eb-8c22cf093117", map[string]interface{}{}),
			},
			error:                "content is synthetic publication",
			expectedNotification: nil,
		},
		{
			name:                        "unmarshallable message body will cause errors",
			contentOriginAllowlistRegex: regexp.MustCompile(`^http://(wordpress|upp)-(article|content)-(transformer|mapper|validator)(-pr|-iw)?(-uk-.*)?\.svc\.ft\.com(:\d{2,5})?/(content|audio)/[\w-]+.*$`),
			allowedContentTypes:         []string{"Audio", "Article", "LiveBlogPost", "LiveBlogPackage", "Content"},
			msg: kafka.FTMessage{
				Headers: map[string]string{"X-Request-Id": "tid_1234"},
				Body:    "unmarshallable",
			},
			expectedNotification: nil,
			error:                "error unmarshaling event: invalid character 'u' looking for beginning of value",
		},
		{
			name:                        "content type out of white list will skip mapping",
			contentOriginAllowlistRegex: regexp.MustCompile(`^http://(wordpress|upp)-(article|content)-(mapper|validator)\.svc\.ft\.com(:\\d{2,5})?/content/[\w-]+.*$`),
			allowedContentTypes:         []string{"Article"},
			msg: kafka.FTMessage{
				Headers: map[string]string{"X-Request-Id": "tid_1234"},
				Body:    generateRequestBody("http://upp-content-validator.svc.ft.com/audio/811e0591-5c71-4457-b8eb-8c22cf093117", map[string]interface{}{}),
			},
			error:                "contentURI http://upp-content-validator.svc.ft.com/audio/811e0591-5c71-4457-b8eb-8c22cf093117 is not exportable",
			expectedNotification: nil,
		},
		{
			name:                        "content uri with invalid uuid will cause error",
			contentOriginAllowlistRegex: regexp.MustCompile(`^http://(wordpress|upp)-(article|content)-(transformer|mapper|validator)(-pr|-iw)?(-uk-.*)?\.svc\.ft\.com(:\d{2,5})?/(content|audio)/[\w-]+.*$`),
			allowedContentTypes:         []string{"Audio", "Article", "LiveBlogPost", "LiveBlogPackage", "Content"},
			msg: kafka.FTMessage{
				Headers: map[string]string{"X-Request-Id": "tid_1234"},
				Body:    generateRequestBody("http://upp-content-validator.svc.ft.com/content/invalidUUID", map[string]interface{}{}),
			},
			expectedNotification: nil,
			error:                "error building notification: contentURI does not contain a UUID",
		},
		{
			name:                        "invalid payload type will cause error",
			contentOriginAllowlistRegex: regexp.MustCompile(`^http://(wordpress|upp)-(article|content)-(transformer|mapper|validator)(-pr|-iw)?(-uk-.*)?\.svc\.ft\.com(:\d{2,5})?/(content|audio)/[\w-]+.*$`),
			allowedContentTypes:         []string{"Audio", "Article", "LiveBlogPost", "LiveBlogPackage", "Content"},
			msg: kafka.FTMessage{
				Headers: map[string]string{"X-Request-Id": "tid_1234"},
				Body:    generateRequestBody("http://upp-content-validator.svc.ft.com/content/811e0591-5c71-4457-b8eb-8c22cf093117", []interface{}{"type", "Article"}),
			},
			expectedNotification: nil,
			error:                "error building notification: invalid payload type: []interface {}",
		},
		{
			name:                        "non-article content type is processed properly",
			contentOriginAllowlistRegex: regexp.MustCompile(`^http://(wordpress|upp)-(article|content)-(transformer|mapper|validator)(-pr|-iw)?(-uk-.*)?\.svc\.ft\.com(:\d{2,5})?/(content|audio)/[\w-]+.*$`),
			allowedContentTypes:         []string{"Audio", "Article", "LiveBlogPost", "LiveBlogPackage", "Content"},
			msg: kafka.FTMessage{
				Headers: map[string]string{"X-Request-Id": "tid_1234"},
				Body: generateRequestBody("http://upp-content-validator.svc.ft.com/content/811e0591-5c71-4457-b8eb-8c22cf093117", map[string]interface{}{
					"type": "LiveBlogPackage",
				}),
			},
			expectedNotification: &Notification{
				Tid:    "tid_1234",
				EvType: UPDATE,
				Stub: content.Stub{
					UUID:        "811e0591-5c71-4457-b8eb-8c22cf093117",
					ContentType: "LiveBlogPackage",
				},
			},
		},
		{
			name:                        "unallowed content type will skip mapping",
			contentOriginAllowlistRegex: regexp.MustCompile(`^http://(wordpress|upp)-(article|content)-(mapper|validator)\.svc\.ft\.com(:\\d{2,5})?/content/[\w-]+.*$`),
			allowedContentTypes:         []string{"Article"},
			msg: kafka.FTMessage{
				Headers: map[string]string{"X-Request-Id": "tid_1234"},
				Body: generateRequestBody("http://upp-content-validator.svc.ft.com/content/811e0591-5c71-4457-b8eb-8c22cf093117", map[string]interface{}{
					"type": "LiveBlogPackage",
				}),
			},
			error:                "content type LiveBlogPackage is not exportable",
			expectedNotification: nil,
		},
		{
			name:                        "missing content type will skip mapping",
			contentOriginAllowlistRegex: regexp.MustCompile(`^http://(wordpress|upp)-(article|content)-(mapper|validator)\.svc\.ft\.com(:\\d{2,5})?/content/[\w-]+.*$`),
			allowedContentTypes:         []string{"Article"},
			msg: kafka.FTMessage{
				Headers: map[string]string{"X-Request-Id": "tid_1234"},
				Body:    generateRequestBody("http://upp-content-validator.svc.ft.com/content/811e0591-5c71-4457-b8eb-8c22cf093117", map[string]interface{}{}),
			},
			expectedNotification: nil,
			error:                "content type  is not exportable",
		},
		{
			name:                        "content type of unexpected type will skip mapping",
			contentOriginAllowlistRegex: regexp.MustCompile(`^http://(wordpress|upp)-(article|content)-(mapper|validator)\.svc\.ft\.com(:\\d{2,5})?/content/[\w-]+.*$`),
			allowedContentTypes:         []string{"Article"},
			msg: kafka.FTMessage{
				Headers: map[string]string{"X-Request-Id": "tid_1234"},
				Body: generateRequestBody("http://upp-content-validator.svc.ft.com/content/811e0591-5c71-4457-b8eb-8c22cf093117", map[string]interface{}{
					"type": 56,
				}),
			},
			error:                "content type  is not exportable",
			expectedNotification: nil,
		},
		{
			name:                        "canBeDistributed with value different than yes will skip mapping",
			contentOriginAllowlistRegex: regexp.MustCompile(`^http://(wordpress|upp)-(article|content)-(mapper|validator)\.svc\.ft\.com(:\\d{2,5})?/content/[\w-]+.*$`),
			allowedContentTypes:         []string{"Article"},
			msg: kafka.FTMessage{
				Headers: map[string]string{"X-Request-Id": "tid_1234"},
				Body: generateRequestBody("http://upp-content-validator.svc.ft.com/content/811e0591-5c71-4457-b8eb-8c22cf093117", map[string]interface{}{
					"type":             "Article",
					"canBeDistributed": "no",
				}),
			},
			error:                "content is not distributable",
			expectedNotification: nil,
		},
		{
			name:                        "valid message will map to valid notification",
			contentOriginAllowlistRegex: regexp.MustCompile(`^http://(wordpress|upp)-(article|content)-(mapper|validator)\.svc\.ft\.com(:\\d{2,5})?/content/[\w-]+.*$`),
			allowedContentTypes:         []string{"Article"},
			msg: kafka.FTMessage{
				Headers: map[string]string{"X-Request-Id": "tid_1234"},
				Body: generateRequestBody("http://upp-content-validator.svc.ft.com/content/811e0591-5c71-4457-b8eb-8c22cf093117", map[string]interface{}{
					"type":             "Article",
					"canBeDistributed": "yes",
				}),
			},
			expectedNotification: &Notification{
				Tid:    "tid_1234",
				EvType: UPDATE,
				Stub: content.Stub{
					UUID:             "811e0591-5c71-4457-b8eb-8c22cf093117",
					ContentType:      "Article",
					CanBeDistributed: func(s string) *string { return &s }("yes"),
				},
			},
		},
		{
			name:                        "valid message will map to valid notification - delete event",
			contentOriginAllowlistRegex: regexp.MustCompile(`^http://(wordpress|upp)-(article|content)-(mapper|validator)\.svc\.ft\.com(:\\d{2,5})?/content/[\w-]+.*$`),
			allowedContentTypes:         []string{"Article"},
			msg: kafka.FTMessage{
				Headers: map[string]string{"X-Request-Id": "tid_1234"},
				Body: generateRequestBody("http://upp-content-validator.svc.ft.com/content/811e0591-5c71-4457-b8eb-8c22cf093117", map[string]interface{}{
					"type":    "Article",
					"deleted": true,
				}),
			},
			expectedNotification: &Notification{
				Tid:    "tid_1234",
				EvType: DELETE,
				Stub: content.Stub{
					UUID:        "811e0591-5c71-4457-b8eb-8c22cf093117",
					ContentType: "Article",
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mapper := NewMessageMapper(test.contentOriginAllowlistRegex, test.allowedContentTypes)
			n, err := mapper.mapNotification(test.msg)
			if test.error != "" {
				require.EqualError(t, err, test.error)
				return
			}

			require.NoError(t, err)

			cmpOpts := cmpopts.IgnoreFields(Notification{}, "Stub.Date", "Terminator")
			assert.Truef(t, cmp.Equal(test.expectedNotification, n, cmpOpts), "Mapped notification differs from expected:\n%s", cmp.Diff(test.expectedNotification, n, cmpOpts))
		})
	}
}
