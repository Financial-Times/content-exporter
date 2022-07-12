package main

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Financial-Times/go-logger/v2"
	log "github.com/sirupsen/logrus"

	"github.com/Financial-Times/content-exporter/content"
	"github.com/Financial-Times/content-exporter/db"
	"github.com/Financial-Times/content-exporter/export"
	"github.com/Financial-Times/content-exporter/queue"
	"github.com/Financial-Times/content-exporter/web"
	health "github.com/Financial-Times/go-fthealth/v1_1"
	"github.com/Financial-Times/http-handlers-go/httphandlers"
	"github.com/Financial-Times/kafka-client-go/v3"
	status "github.com/Financial-Times/service-status-go/httphandlers"

	"github.com/gorilla/mux"
	cli "github.com/jawher/mow.cli"
	"github.com/rcrowley/go-metrics"
	"github.com/sethgrid/pester"
)

const (
	serviceName    = "content-exporter"
	appDescription = "Exports content from DB and sends to S3"
)

func main() {
	app := cli.App(serviceName, appDescription)

	appSystemCode := app.String(cli.StringOpt{
		Name:   "app-system-code",
		Value:  "content-exporter",
		Desc:   "System Code of the application",
		EnvVar: "APP_SYSTEM_CODE",
	})
	appName := app.String(cli.StringOpt{
		Name:   "app-name",
		Value:  "content-exporter",
		Desc:   "Application name",
		EnvVar: "APP_NAME",
	})
	port := app.String(cli.StringOpt{
		Name:   "port",
		Value:  "8080",
		Desc:   "Port to listen on",
		EnvVar: "APP_PORT",
	})
	mongos := app.String(cli.StringOpt{
		Name:   "mongoConnection",
		Value:  "",
		Desc:   "Mongo addresses to connect to in format: host1:port1,host2:port2,...]",
		EnvVar: "MONGO_CONNECTION",
	})
	enrichedContentBaseURL := app.String(cli.StringOpt{
		Name:   "enrichedContentBaseURL",
		Value:  "http://localhost:8080",
		Desc:   "Base URL to enriched content endpoint",
		EnvVar: "ENRICHED_CONTENT_BASE_URL",
	})
	enrichedContentHealthURL := app.String(cli.StringOpt{
		Name:   "enrichedContentHealthURL",
		Value:  "http://localhost:8080/__gtg",
		Desc:   "Health URL to enriched content endpoint",
		EnvVar: "ENRICHED_CONTENT_HEALTH_URL",
	})
	s3WriterBaseURL := app.String(cli.StringOpt{
		Name:   "s3WriterBaseURL",
		Value:  "http://localhost:8080",
		Desc:   "Base URL to S3 writer endpoint",
		EnvVar: "S3_WRITER_BASE_URL",
	})
	s3WriterHealthURL := app.String(cli.StringOpt{
		Name:   "s3WriterHealthURL",
		Value:  "http://localhost:8080/__gtg",
		Desc:   "Health URL to S3 writer endpoint",
		EnvVar: "S3_WRITER_HEALTH_URL",
	})
	xPolicyHeaderValues := app.String(cli.StringOpt{
		Name:   "xPolicyHeaderValues",
		Desc:   "Values for X-Policy header separated by comma, e.g. INCLUDE_RICH_CONTENT,EXPAND_IMAGES",
		EnvVar: "X_POLICY_HEADER_VALUES",
	})
	authorization := app.String(cli.StringOpt{
		Name:   "authorization",
		Desc:   "Authorization for enrichedcontent endpoint, needed only when calling the endpoint via Varnish",
		EnvVar: "AUTHORIZATION",
	})
	consumerAddrs := app.String(cli.StringOpt{
		Name:   "kafka-addr",
		Desc:   "Comma separated kafka hosts for message consuming.",
		EnvVar: "KAFKA_ADDRS",
	})
	consumerGroupID := app.String(cli.StringOpt{
		Name:   "group-id",
		Desc:   "Kafka qroup id used for message consuming.",
		EnvVar: "GROUP_ID",
	})
	topic := app.String(cli.StringOpt{
		Name:   "topic",
		Desc:   "Kafka topic to read from.",
		EnvVar: "TOPIC",
	})
	delayForNotification := app.Int(cli.IntOpt{
		Name:   "delayForNotification",
		Value:  30,
		Desc:   "Delay in seconds for notifications to being handled",
		EnvVar: "DELAY_FOR_NOTIFICATION",
	})
	contentRetrievalThrottle := app.Int(cli.IntOpt{
		Name:   "contentRetrievalThrottle",
		Value:  0,
		Desc:   "Delay in milliseconds between content retrieval calls",
		EnvVar: "CONTENT_RETRIEVAL_THROTTLE",
	})
	contentOriginAllowlist := app.String(cli.StringOpt{
		Name:   "contentOriginAllowlist",
		Desc:   `The Content Origin allowlist for incoming notifications - i.e. ^http://.*-transformer-(pr|iw)-uk-.*\.svc\.ft\.com(:\d{2,5})?/content/[\w-]+.*$`,
		EnvVar: "CONTENT_ORIGIN_ALLOWLIST",
	})
	logLevel := app.String(cli.StringOpt{
		Name:   "logLevel",
		Value:  "INFO",
		Desc:   "Logging level (DEBUG, INFO, WARN, ERROR)",
		EnvVar: "LOG_LEVEL",
	})
	isIncExportEnabled := app.Bool(cli.BoolOpt{
		Name:   "is-inc-export-enabled",
		Value:  false,
		Desc:   "Flag representing whether incremental exports should run.",
		EnvVar: "IS_INC_EXPORT_ENABLED",
	})
	maxGoRoutines := app.Int(cli.IntOpt{
		Name:   "maxGoRoutines",
		Value:  100,
		Desc:   "Maximum goroutines to allocate for kafka message handling",
		EnvVar: "MAX_GO_ROUTINES",
	})
	allowedContentTypes := app.Strings(cli.StringsOpt{
		Name:   "allowed-content-types",
		Value:  []string{},
		Desc:   `Comma-separated list of ContentTypes`,
		EnvVar: "ALLOWED_CONTENT_TYPES",
	})

	app.Before = func() {
		if err := checkMongoURLs(*mongos); err != nil {
			app.PrintHelp()
			log.Fatalf("Mongo connection is not set correctly: %s", err.Error())
		}
		_, err := regexp.Compile(*contentOriginAllowlist)
		if err != nil {
			app.PrintHelp()
			log.WithError(err).Fatal("contentOriginAllowlist regex MUST compile!")
		}
	}

	app.Action = func() {

		log := logger.NewUPPLogger(serviceName, *logLevel)

		log.WithField("event", "service_started").WithField("app-name", *appName).Info("Service started")

		mongo := db.NewMongoDatabase(*mongos, 100)

		tr := &http.Transport{
			MaxIdleConnsPerHost: 128,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
		}
		c := &http.Client{
			Transport: tr,
			Timeout:   30 * time.Second,
		}
		client := pester.NewExtendedClient(c)
		client.Backoff = pester.ExponentialBackoff
		client.MaxRetries = 3
		client.Concurrency = 1

		fetcher := &content.EnrichedContentFetcher{
			Client:                   client,
			EnrichedContentBaseURL:   *enrichedContentBaseURL,
			EnrichedContentHealthURL: *enrichedContentHealthURL,
			XPolicyHeaderValues:      *xPolicyHeaderValues,
			Authorization:            *authorization,
		}
		uploader := &content.S3Updater{Client: client, S3WriterBaseURL: *s3WriterBaseURL, S3WriterHealthURL: *s3WriterHealthURL}

		exporter := content.NewExporter(fetcher, uploader)
		fullExporter := export.NewFullExporter(20, exporter)
		locker := export.NewLocker()
		var kafkaListener *queue.KafkaListener
		if *isIncExportEnabled {
			kafkaListener = prepareIncrementalExport(log, consumerAddrs, consumerGroupID, topic, contentOriginAllowlist, *allowedContentTypes, exporter, delayForNotification, locker, maxGoRoutines)
			go kafkaListener.ConsumeMessages()
			defer kafkaListener.StopConsumingMessages()
		} else {
			log.Warn("INCREMENTAL export is not enabled")
		}

		healthService := newHealthService(
			&healthConfig{
				appSystemCode:          *appSystemCode,
				appName:                *appName,
				port:                   *port,
				db:                     mongo,
				enrichedContentFetcher: fetcher,
				s3Uploader:             uploader,
				queueHandler:           kafkaListener,
			}, fullExporter)

		go serveEndpoints(*appSystemCode, *appName, *port, web.NewRequestHandler(fullExporter, content.NewMongoInquirer(mongo), locker, *isIncExportEnabled, *contentRetrievalThrottle), healthService)

		waitForSignal()
		log.Info("Gracefully shut down")

	}

	err := app.Run(os.Args)
	if err != nil {
		log.Errorf("App could not start, error=[%s]\n", err)
		return
	}
}

func prepareIncrementalExport(logger *logger.UPPLogger, consumerAddrs *string, consumerGroupID *string, topic *string, contentOriginAllowlist *string, allowedContentTypes []string, exporter *content.Exporter, delayForNotification *int, locker *export.Locker, maxGoRoutines *int) *queue.KafkaListener {
	config := kafka.ConsumerConfig{
		BrokersConnectionString: *consumerAddrs,
		ConsumerGroup:           *consumerGroupID,
		ConnectionRetryInterval: time.Minute,
	}
	topics := []*kafka.Topic{
		kafka.NewTopic(*topic),
	}
	messageConsumer := kafka.NewConsumer(config, topics, logger)

	contentOriginAllowListRegex, err := regexp.Compile(*contentOriginAllowlist)
	if err != nil {
		log.WithError(err).Fatal("contentOriginAllowlist regex MUST compile!")
	}

	kafkaMessageHandler := queue.NewKafkaContentNotificationHandler(exporter, *delayForNotification)
	kafkaMessageMapper := queue.NewKafkaMessageMapper(contentOriginAllowListRegex, allowedContentTypes)
	kafkaListener := queue.NewKafkaListener(messageConsumer, kafkaMessageHandler, kafkaMessageMapper, locker, *maxGoRoutines)

	return kafkaListener
}

func serveEndpoints(appSystemCode string, appName string, port string, requestHandler *web.RequestHandler,
	healthService *healthService) {

	serveMux := http.NewServeMux()

	hc := health.HealthCheck{SystemCode: appSystemCode, Name: appName, Description: appDescription, Checks: healthService.checks}
	serveMux.HandleFunc(healthPath, health.Handler(hc))
	serveMux.HandleFunc(status.GTGPath, status.NewGoodToGoHandler(healthService.GTG))
	serveMux.HandleFunc(status.BuildInfoPath, status.BuildInfoHandler)

	servicesRouter := mux.NewRouter()
	servicesRouter.HandleFunc("/export", requestHandler.Export).Methods(http.MethodPost)
	servicesRouter.HandleFunc("/jobs/{jobID}", requestHandler.GetJob).Methods(http.MethodGet)
	servicesRouter.HandleFunc("/jobs", requestHandler.GetRunningJobs).Methods(http.MethodGet)

	var monitoringRouter http.Handler = servicesRouter
	monitoringRouter = httphandlers.TransactionAwareRequestLoggingHandler(log.StandardLogger(), monitoringRouter)
	monitoringRouter = httphandlers.HTTPMetricsHandler(metrics.DefaultRegistry, monitoringRouter)

	serveMux.Handle("/", monitoringRouter)

	server := &http.Server{
		Addr:         ":" + port,
		Handler:      serveMux,
		ReadTimeout:  60 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		if err := server.ListenAndServe(); err != nil {
			log.Infof("HTTP server closing with message: %v", err)
		}
		wg.Done()
	}()

	waitForSignal()
	log.Infof("[Shutdown] content-exporter is shutting down")

	if err := server.Close(); err != nil {
		log.Errorf("Unable to stop http server: %v", err)
	}

	wg.Wait()
}

func waitForSignal() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
}

func checkMongoURLs(providedMongoURLs string) error {
	if providedMongoURLs == "" {
		return errors.New("MongoDB urls are missing")
	}

	mongoURLs := strings.Split(providedMongoURLs, ",")

	for _, mongoURL := range mongoURLs {
		host, port, err := net.SplitHostPort(mongoURL)
		if err != nil {
			return fmt.Errorf("cannot split MongoDB URL: %s into host and port. Error is: %s", mongoURL, err.Error())
		}
		if host == "" || port == "" {
			return fmt.Errorf("one of the MongoDB URLs is incomplete: %s. It should have host and port", mongoURL)
		}
	}

	return nil
}
