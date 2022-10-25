package main

import (
	"fmt"
	"reflect"

	health "github.com/Financial-Times/go-fthealth/v1_1"
	"github.com/Financial-Times/service-status-go/gtg"
)

const (
	healthPath        = "/__health"
	fullExportMessage = "Service is currently performing a full export and lag is expected"
)

type healthService struct {
	healthChecks []health.Check
	gtgChecks    []gtg.StatusChecker
}

type healthChecker interface {
	CheckHealth() (string, error)
}

type queueChecker interface {
	healthChecker
	MonitorCheck() (string, error)
}

type exportStatusManager interface {
	IsFullExportRunning() bool
}

func newHealthService(dbChecker, readChecker, writeChecker healthChecker, queueChecker queueChecker, statusManager exportStatusManager) *healthService {
	mongoCheck := newMongoCheck(dbChecker)
	readerCheck := newReadEndpointCheck(readChecker)
	writerCheck := newS3WriterCheck(writeChecker)

	healthChecks := []health.Check{mongoCheck, readerCheck, writerCheck}
	gtgChecks := []health.Check{mongoCheck, readerCheck, writerCheck}

	if !reflect.ValueOf(queueChecker).IsNil() {
		kafkaConnectivityCheck := newKafkaConnectivityCheck(queueChecker)
		kafkaMonitorCheck := newKafkaMonitorCheck(queueChecker, statusManager)

		healthChecks = append(healthChecks, kafkaConnectivityCheck, kafkaMonitorCheck)
		gtgChecks = append(gtgChecks, kafkaConnectivityCheck)
	}

	gtgCheckers := make([]gtg.StatusChecker, 0, len(gtgChecks))
	for _, c := range gtgChecks {
		gtgCheckers = append(gtgCheckers, gtgCheck(c))
	}

	return &healthService{
		healthChecks: healthChecks,
		gtgChecks:    gtgCheckers,
	}
}

func newMongoCheck(checker healthChecker) health.Check {
	return health.Check{
		Name:             "CheckConnectivityToMongoDatabase",
		BusinessImpact:   "No Business Impact.",
		PanicGuide:       "https://runbooks.in.ft.com/content-exporter",
		Severity:         2,
		TechnicalSummary: "The service is unable to connect to MongoDB. FULL or TARGETED export won't work because of this",
		Checker:          checker.CheckHealth,
	}
}

func newReadEndpointCheck(checker healthChecker) health.Check {
	return health.Check{
		Name:             "CheckConnectivityToApiPolicyComponent",
		BusinessImpact:   "No Business Impact.",
		PanicGuide:       "https://runbooks.in.ft.com/content-exporter",
		Severity:         2,
		TechnicalSummary: "The service is unable to connect to Api Policy Component. Neither FULL nor INCREMENTAL or TARGETED export won't work because of this",
		Checker:          checker.CheckHealth,
	}
}

func newS3WriterCheck(checker healthChecker) health.Check {
	return health.Check{
		Name:             "CheckConnectivityToContentRWS3",
		BusinessImpact:   "No Business Impact.",
		PanicGuide:       "https://runbooks.in.ft.com/content-exporter",
		Severity:         2,
		TechnicalSummary: "The service is unable to connect to Content-RW-S3. Neither FULL nor INCREMENTAL or TARGETED export won't work because of this",
		Checker:          checker.CheckHealth,
	}
}

func newKafkaConnectivityCheck(checker healthChecker) health.Check {
	return health.Check{
		Name:             "CheckConnectivityToKafka",
		BusinessImpact:   "No Business Impact.",
		PanicGuide:       "https://runbooks.in.ft.com/content-exporter",
		Severity:         2,
		TechnicalSummary: "The service is unable to connect to Kafka. INCREMENTAL export won't work because of this",
		Checker:          checker.CheckHealth,
	}
}

func newKafkaMonitorCheck(checker queueChecker, statusManager exportStatusManager) health.Check {
	return health.Check{
		Name:             "KafkaClientLag",
		BusinessImpact:   "No Business Impact.",
		PanicGuide:       "https://runbooks.in.ft.com/content-exporter",
		Severity:         3,
		TechnicalSummary: "Messages awaiting handling exceed the configured lag tolerance. Check if Kafka consumer is stuck.",
		Checker: func() (string, error) {
			return customKafkaMonitorCheck(checker, statusManager)
		},
	}
}

func customKafkaMonitorCheck(monitoringChecker queueChecker, statusManager exportStatusManager) (string, error) {
	status, err := monitoringChecker.MonitorCheck()
	if err == nil || !statusManager.IsFullExportRunning() {
		return status, err
	}
	msg := fmt.Sprintf("%s: %s", fullExportMessage, err.Error())
	return msg, nil
}

func (service *healthService) GTG() gtg.Status {
	return gtg.FailFastParallelCheck(service.gtgChecks)()
}

func gtgCheck(check health.Check) func() gtg.Status {
	return func() gtg.Status {
		if _, err := check.Checker(); err != nil {
			return gtg.Status{GoodToGo: false, Message: err.Error()}
		}

		return gtg.Status{GoodToGo: true}
	}
}
