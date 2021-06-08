package queue

import (
	"encoding/json"
	"regexp"
	"testing"

	"github.com/Financial-Times/content-exporter/content"
	"github.com/Financial-Times/kafka-client-go/kafka"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func NewComplexMessageMapper() MessageMapper {
	return NewKafkaMessageMapper(regexp.MustCompile("^http://(methode|wordpress|upp)-(article|content)-(transformer|mapper|validator)(-pr|-iw)?(-uk-.*)?\\.svc\\.ft\\.com(:\\d{2,5})?/(content|audio)/[\\w-]+.*$"))
}

func testMapDeleteMessageSuccessfully(t *testing.T, ev event, testUUID string) {
	messageMapper := NewComplexMessageMapper()

	body, err := json.Marshal(ev)
	require.NoError(t, err)
	n, err := messageMapper.MapNotification(kafka.FTMessage{Body: string(body), Headers: map[string]string{"X-Request-Id": "tid_1234"}})
	assert.NoError(t, err)
	assert.Equal(t, DELETE, n.EvType)
	assert.Equal(t, "tid_1234", n.Tid)
	assert.Equal(t, testUUID, n.Stub.Uuid)
	assert.Equal(t, content.DefaultDate, n.Stub.Date)
	assert.Nil(t, n.Stub.CanBeDistributed)
}

func TestKafkaMessageMapperMapDeleteMessageSuccessfully(t *testing.T) {
	testUUID := uuid.New()
	testMapDeleteMessageSuccessfully(t, event{
		ContentURI: "http://methode-article-mapper.svc.ft.com/content/" + testUUID,
		Payload:    map[string]interface{}{"deleted": true}}, testUUID)
}

func TestKafkaMessageMapperMapDeleteAudioMessageSuccessfully(t *testing.T) {
	testUUID := uuid.New()
	testMapDeleteMessageSuccessfully(t, event{
		ContentURI: "http://upp-content-validator.svc.ft.com/audio/" + testUUID,
		Payload:    map[string]interface{}{"deleted": true}}, testUUID)
}

func TestKafkaMessageMapperMapUpdateMessageSuccessfully(t *testing.T) {
	messageMapper := NewComplexMessageMapper()
	testUUID := uuid.New()
	body, err := json.Marshal(event{
		ContentURI: "http://methode-article-mapper.svc.ft.com/content/" + testUUID,
		Payload:    map[string]interface{}{"title": "This is a title", "type": "Article"}})
	require.NoError(t, err)

	n, err := messageMapper.MapNotification(kafka.FTMessage{Body: string(body), Headers: map[string]string{"X-Request-Id": "tid_1234"}})

	assert.NoError(t, err)
	assert.Equal(t, UPDATE, n.EvType)
	assert.Equal(t, "tid_1234", n.Tid)
	assert.Equal(t, testUUID, n.Stub.Uuid)
	assert.Equal(t, content.DefaultDate, n.Stub.Date)
	assert.Nil(t, n.Stub.CanBeDistributed)
}

func TestKafkaMessageMapperMapUpdateCanBeDistributedYes(t *testing.T) {
	messageMapper := NewComplexMessageMapper()
	testUUID := uuid.New()
	body, err := json.Marshal(event{
		ContentURI: "http://methode-article-mapper.svc.ft.com/content/" + testUUID,
		Payload:    map[string]interface{}{"title": "This is a title", "type": "Article", "canBeDistributed": "yes"}})
	require.NoError(t, err)

	n, err := messageMapper.MapNotification(kafka.FTMessage{Body: string(body), Headers: map[string]string{"X-Request-Id": "tid_1234"}})

	assert.NoError(t, err)
	assert.Equal(t, UPDATE, n.EvType)
	assert.Equal(t, "tid_1234", n.Tid)
	assert.Equal(t, testUUID, n.Stub.Uuid)
	assert.Equal(t, content.DefaultDate, n.Stub.Date)
	expectedCanBeDistributed := new(string)
	*expectedCanBeDistributed = canBeDistributedYes
	assert.Equal(t, expectedCanBeDistributed, n.Stub.CanBeDistributed)
}

func TestKafkaMessageMapperMapUpdateCanBeDistributedVerify(t *testing.T) {
	messageMapper := NewComplexMessageMapper()
	testUUID := uuid.New()
	body, err := json.Marshal(event{
		ContentURI: "http://methode-article-mapper.svc.ft.com/content/" + testUUID,
		Payload:    map[string]interface{}{"title": "This is a title", "type": "Article", "canBeDistributed": "verify"}})
	require.NoError(t, err)

	n, err := messageMapper.MapNotification(kafka.FTMessage{Body: string(body), Headers: map[string]string{"X-Request-Id": "tid_1234"}})

	assert.NoError(t, err)
	assert.Nil(t, n)
}

func TestKafkaMessageMapperMapUpdateMessageSuccessfullyForSpark(t *testing.T) {
	messageMapper := NewComplexMessageMapper()
	testUUID := uuid.New()
	body, err := json.Marshal(event{
		ContentURI: "http://upp-content-validator.svc.ft.com/content/" + testUUID,
		Payload:    map[string]interface{}{"title": "This is a title", "type": "Article"}})
	require.NoError(t, err)

	n, err := messageMapper.MapNotification(kafka.FTMessage{Body: string(body), Headers: map[string]string{"X-Request-Id": "tid_1234"}})

	assert.NoError(t, err)
	assert.Equal(t, UPDATE, n.EvType)
	assert.Equal(t, "tid_1234", n.Tid)
	assert.Equal(t, testUUID, n.Stub.Uuid)
	assert.Equal(t, content.DefaultDate, n.Stub.Date)
	assert.Nil(t, n.Stub.CanBeDistributed)
}

func TestKafkaMessageMapperMapNotificationNotInWhiteListError(t *testing.T) {
	messageMapper := NewComplexMessageMapper()
	body, err := json.Marshal(event{
		ContentURI: "http://wordpress-article-mapper/content/",
		Payload:    map[string]interface{}{"title": "This is a title", "type": "Article"}})
	require.NoError(t, err)

	n, err := messageMapper.MapNotification(kafka.FTMessage{Body: string(body), Headers: map[string]string{"X-Request-Id": "tid_1234"}})

	assert.NoError(t, err)
	assert.Nil(t, n)
}

func TestKafkaMessageMapperMapNotificationSyntheticError(t *testing.T) {
	messageMapper := NewComplexMessageMapper()
	body, err := json.Marshal(event{
		ContentURI: "http://methode-article-mapper.svc.ft.com/content/",
		Payload:    map[string]interface{}{"title": "This is a title", "type": "Article"}})
	require.NoError(t, err)

	n, err := messageMapper.MapNotification(kafka.FTMessage{Body: string(body), Headers: map[string]string{"X-Request-Id": "SYNTH_tid_1234"}})

	assert.NoError(t, err)
	assert.Nil(t, n)
}

func TestKafkaMessageMapperMapNotificationMessageParseError(t *testing.T) {
	messageMapper := NewComplexMessageMapper()

	n, err := messageMapper.MapNotification(kafka.FTMessage{Body: "random-text", Headers: map[string]string{"X-Request-Id": "SYNTH_tid_1234"}})

	assert.Error(t, err)
	assert.Equal(t, "invalid character 'r' looking for beginning of value", err.Error())
	assert.Nil(t, n)
}

func TestKafkaMessageMapperMapNotificationInvalidPayloadType(t *testing.T) {
	testUUID := uuid.New()
	body, err := json.Marshal(event{
		ContentURI: "http://methode-article-mapper.svc.ft.com/content/" + testUUID,
		Payload:    []interface{}{"title", "This is a title"}})
	require.NoError(t, err)

	messageMapper := NewComplexMessageMapper()
	n, err := messageMapper.MapNotification(kafka.FTMessage{Body: string(body), Headers: map[string]string{"X-Request-Id": "tid_1234"}})

	require.Error(t, err)
	assert.Equal(t, "invalid payload type: []interface {}", err.Error())
	assert.Nil(t, n)
}
