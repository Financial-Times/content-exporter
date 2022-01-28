package queue

import (
	"encoding/json"
	"regexp"
	"testing"

	"github.com/Financial-Times/content-exporter/content"
	"github.com/Financial-Times/kafka-client-go/kafka"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var exporterRegex = regexp.MustCompile(`^http://(wordpress|upp)-(article|content)-(mapper|validator)\.svc\.ft\.com(:\\d{2,5})?/content/[\w-]+.*$`)
var fullExporterRegex = regexp.MustCompile(`^http://(wordpress|upp)-(article|content)-(transformer|mapper|validator)(-pr|-iw)?(-uk-.*)?\.svc\.ft\.com(:\d{2,5})?/(content|audio)/[\w-]+.*$`)

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
		name                 string
		fullExporter         bool
		msg                  kafka.FTMessage
		expectedNotification *Notification
		error                string
	}{
		{
			name:         "Test that synthetic transaction ID will skip mapping",
			fullExporter: true,
			msg: kafka.FTMessage{
				Headers: map[string]string{"X-Request-Id": "SYNTH_REQ_MON1"},
				Body:    generateRequestBody("http://upp-content-validator.svc.ft.com/content/811e0591-5c71-4457-b8eb-8c22cf093117", map[string]interface{}{}),
			},
			expectedNotification: nil,
		},
		{
			name:         "Test that unmarshallable message body will cause errors",
			fullExporter: true,
			msg: kafka.FTMessage{
				Headers: map[string]string{"X-Request-Id": "tid_1234"},
				Body:    "unmarshallable",
			},
			expectedNotification: nil,
			error:                "invalid character 'u' looking for beginning of value",
		},
		{
			name:         "Test that content type out of white list will skip mapping",
			fullExporter: false,
			msg: kafka.FTMessage{
				Headers: map[string]string{"X-Request-Id": "tid_1234"},
				Body:    generateRequestBody("http://upp-content-validator.svc.ft.com/audio/811e0591-5c71-4457-b8eb-8c22cf093117", map[string]interface{}{}),
			},
			expectedNotification: nil,
		},
		{
			name:         "Test that content uri with invalid uuid will cause error",
			fullExporter: true,
			msg: kafka.FTMessage{
				Headers: map[string]string{"X-Request-Id": "tid_1234"},
				Body:    generateRequestBody("http://upp-content-validator.svc.ft.com/content/invalidUUID", map[string]interface{}{}),
			},
			expectedNotification: nil,
			error:                "contentURI does not contain a UUID",
		},
		{
			name:         "Test that invalid payload type will cause error",
			fullExporter: true,
			msg: kafka.FTMessage{
				Headers: map[string]string{"X-Request-Id": "tid_1234"},
				Body:    generateRequestBody("http://upp-content-validator.svc.ft.com/content/811e0591-5c71-4457-b8eb-8c22cf093117", []interface{}{"type", "Article"}),
			},
			expectedNotification: nil,
			error:                "invalid payload type: []interface {}",
		},
		{
			name:         "Test that non-article content type is processed properly",
			fullExporter: true,
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
			name:         "Test that unallowed content type will skip mapping",
			fullExporter: false,
			msg: kafka.FTMessage{
				Headers: map[string]string{"X-Request-Id": "tid_1234"},
				Body: generateRequestBody("http://upp-content-validator.svc.ft.com/content/811e0591-5c71-4457-b8eb-8c22cf093117", map[string]interface{}{
					"type": "LiveBlogPackage",
				}),
			},
			expectedNotification: nil,
		},
		{
			name:         "Test that missing content type will skip mapping",
			fullExporter: false,
			msg: kafka.FTMessage{
				Headers: map[string]string{"X-Request-Id": "tid_1234"},
				Body:    generateRequestBody("http://upp-content-validator.svc.ft.com/content/811e0591-5c71-4457-b8eb-8c22cf093117", map[string]interface{}{}),
			},
			expectedNotification: nil,
		},
		{
			name:         "Test that content type of unexpected type will skip mapping",
			fullExporter: false,
			msg: kafka.FTMessage{
				Headers: map[string]string{"X-Request-Id": "tid_1234"},
				Body: generateRequestBody("http://upp-content-validator.svc.ft.com/content/811e0591-5c71-4457-b8eb-8c22cf093117", map[string]interface{}{
					"type": 56,
				}),
			},
			expectedNotification: nil,
		},
		{
			name:         "Test that canBeDistributed with value different than yes will skip mapping",
			fullExporter: false,
			msg: kafka.FTMessage{
				Headers: map[string]string{"X-Request-Id": "tid_1234"},
				Body: generateRequestBody("http://upp-content-validator.svc.ft.com/content/811e0591-5c71-4457-b8eb-8c22cf093117", map[string]interface{}{
					"type":             "Article",
					"canBeDistributed": "no",
				}),
			},
			expectedNotification: nil,
		},
		{
			name:         "Test that valid message will map to valid notification",
			fullExporter: false,
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
			name:         "Test that valid message will map to valid notification - delete event",
			fullExporter: false,
			msg: kafka.FTMessage{
				Headers: map[string]string{"X-Request-Id": "tid_1234"},
				Body: generateRequestBody("http://upp-content-validator.svc.ft.com/content/811e0591-5c71-4457-b8eb-8c22cf093117", map[string]interface{}{
					"type":             "Article",
					"canBeDistributed": "yes",
					"deleted":          true,
				}),
			},
			expectedNotification: &Notification{
				Tid:    "tid_1234",
				EvType: DELETE,
				Stub: content.Stub{
					UUID:             "811e0591-5c71-4457-b8eb-8c22cf093117",
					ContentType:      "Article",
					CanBeDistributed: func(s string) *string { return &s }("yes"),
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			regex := exporterRegex
			if test.fullExporter {
				regex = fullExporterRegex
			}
			mapper := KafkaMessageMapper{
				regex,
				test.fullExporter,
			}
			n, err := mapper.MapNotification(test.msg)

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
