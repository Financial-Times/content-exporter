package queue

import (
	"encoding/json"
	"regexp"
	"testing"

	"github.com/Financial-Times/content-exporter/content"
	"github.com/Financial-Times/kafka-client-go/v4"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func generateRequestBody(contentURI string, payload payload) string {
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
		allowedContentTypes  []string
		msg                  kafka.FTMessage
		expectedNotification *Notification
		error                string
	}{
		{
			name:                "synthetic transaction ID will skip mapping",
			allowedContentTypes: []string{"Audio", "Article", "LiveBlogPost", "LiveBlogPackage", "Content"},
			msg: kafka.FTMessage{
				Headers: map[string]string{"X-Request-Id": "SYNTH_REQ_MON1"},
				Body:    generateRequestBody("http://upp-content-validator.svc.ft.com/content/811e0591-5c71-4457-b8eb-8c22cf093117", payload{}),
			},
			error:                "content is not exportable: synthetic publication",
			expectedNotification: nil,
		},
		{
			name:                "unmarshallable message body will cause errors",
			allowedContentTypes: []string{"Audio", "Article", "LiveBlogPost", "LiveBlogPackage", "Content"},
			msg: kafka.FTMessage{
				Headers: map[string]string{"X-Request-Id": "tid_1234"},
				Body:    "unmarshallable",
			},
			expectedNotification: nil,
			error:                "error unmarshaling event: invalid character 'u' looking for beginning of value",
		},
		{
			name:                "content type out of white list will skip mapping",
			allowedContentTypes: []string{"Article"},
			msg: kafka.FTMessage{
				Headers: map[string]string{"X-Request-Id": "tid_1234"},
				Body:    generateRequestBody("http://upp-content-validator.svc.ft.com/audio/811e0591-5c71-4457-b8eb-8c22cf093117", payload{}),
			},
			error:                "content is not exportable: uri http://upp-content-validator.svc.ft.com/audio/811e0591-5c71-4457-b8eb-8c22cf093117 not allowed",
			expectedNotification: nil,
		},
		{
			name:                "content uri with invalid uuid will cause error",
			allowedContentTypes: []string{"Audio", "Article", "LiveBlogPost", "LiveBlogPackage", "Content"},
			msg: kafka.FTMessage{
				Headers: map[string]string{"X-Request-Id": "tid_1234"},
				Body:    generateRequestBody("http://upp-content-validator.svc.ft.com/content/invalidUUID", payload{}),
			},
			expectedNotification: nil,
			error:                "error building notification: contentURI does not contain a UUID",
		},
		{
			name:                "non-article content type is processed properly",
			allowedContentTypes: []string{"Audio", "Article", "LiveBlogPost", "LiveBlogPackage", "Content"},
			msg: kafka.FTMessage{
				Headers: map[string]string{"X-Request-Id": "tid_1234"},
				Body: generateRequestBody("http://upp-content-validator.svc.ft.com/content/811e0591-5c71-4457-b8eb-8c22cf093117", payload{
					Type: "LiveBlogPackage",
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
			name:                "unallowed content type will skip mapping",
			allowedContentTypes: []string{"Article"},
			msg: kafka.FTMessage{
				Headers: map[string]string{"X-Request-Id": "tid_1234"},
				Body: generateRequestBody("http://upp-content-validator.svc.ft.com/content/811e0591-5c71-4457-b8eb-8c22cf093117", payload{
					Type: "LiveBlogPackage",
				}),
			},
			error:                "content is not exportable: type LiveBlogPackage not allowed",
			expectedNotification: nil,
		},
		{
			name:                "missing content type will skip mapping",
			allowedContentTypes: []string{"Article"},
			msg: kafka.FTMessage{
				Headers: map[string]string{"X-Request-Id": "tid_1234"},
				Body:    generateRequestBody("http://upp-content-validator.svc.ft.com/content/811e0591-5c71-4457-b8eb-8c22cf093117", payload{}),
			},
			expectedNotification: nil,
			error:                "content is not exportable: type  not allowed",
		},
		{
			name:                "content type of unexpected type will skip mapping",
			allowedContentTypes: []string{"Article"},
			msg: kafka.FTMessage{
				Headers: map[string]string{"X-Request-Id": "tid_1234"},
				Body: generateRequestBody("http://upp-content-validator.svc.ft.com/content/811e0591-5c71-4457-b8eb-8c22cf093117", payload{
					Type: "56",
				}),
			},
			error:                "content is not exportable: type 56 not allowed",
			expectedNotification: nil,
		},
		{
			name:                "canBeDistributed with value different than yes will skip mapping",
			allowedContentTypes: []string{"Article"},
			msg: kafka.FTMessage{
				Headers: map[string]string{"X-Request-Id": "tid_1234"},
				Body: generateRequestBody("http://upp-content-validator.svc.ft.com/content/811e0591-5c71-4457-b8eb-8c22cf093117", payload{
					Type:             "Article",
					CanBeDistributed: "no",
				}),
			},
			error:                "content is not exportable: cannot be distributed",
			expectedNotification: nil,
		},
		{
			name:                "valid message will map to valid notification",
			allowedContentTypes: []string{"Article"},
			msg: kafka.FTMessage{
				Headers: map[string]string{"X-Request-Id": "tid_1234"},
				Body: generateRequestBody("http://upp-content-validator.svc.ft.com/content/811e0591-5c71-4457-b8eb-8c22cf093117", payload{
					Type:             "Article",
					CanBeDistributed: "yes",
				}),
			},
			expectedNotification: &Notification{
				Tid:    "tid_1234",
				EvType: UPDATE,
				Stub: content.Stub{
					UUID:             "811e0591-5c71-4457-b8eb-8c22cf093117",
					ContentType:      "Article",
					CanBeDistributed: "yes",
				},
			},
		},
		{
			name:                "valid message will map to valid notification - delete event",
			allowedContentTypes: []string{"Article"},
			msg: kafka.FTMessage{
				Headers: map[string]string{"X-Request-Id": "tid_1234"},
				Body: generateRequestBody("http://upp-content-validator.svc.ft.com/content/811e0591-5c71-4457-b8eb-8c22cf093117", payload{
					Type:    "Article",
					Deleted: true,
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
	originAllowlistRegex := regexp.MustCompile(`^http://upp-content-validator\.svc\.ft\.com(:\d{2,5})?/content/[\w-]+.*$`)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mapper := NewMessageMapper(originAllowlistRegex, test.allowedContentTypes)
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
