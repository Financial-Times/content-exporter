package main

import (
	"github.com/jawher/mow.cli"
	log "github.com/sirupsen/logrus"
	standardlog "log"

	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/Financial-Times/http-handlers-go/httphandlers"
	"github.com/gorilla/mux"
	"github.com/rcrowley/go-metrics"

	"errors"
	"fmt"
	"github.com/Financial-Times/content-exporter/content"
	"github.com/Financial-Times/content-exporter/db"
	"github.com/Financial-Times/content-exporter/export"
	"github.com/Financial-Times/content-exporter/queue"
	"github.com/Financial-Times/content-exporter/web"
	health "github.com/Financial-Times/go-fthealth/v1_1"
	"github.com/Financial-Times/kafka-client-go/kafka"
	status "github.com/Financial-Times/service-status-go/httphandlers"
	"github.com/Shopify/sarama"
	"github.com/sethgrid/pester"
	"io/ioutil"
	"net"
	"regexp"
	"strings"
	"time"
)

const appDescription = "Exports content from DB and sends to S3"

func init() {
	f := &log.JSONFormatter{
		TimestampFormat: time.RFC3339Nano,
	}

	log.SetFormatter(f)
}

func main() {
	app := cli.App("content-exporter", appDescription)

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
	whitelist := app.String(cli.StringOpt{
		Name:   "whitelist",
		Desc:   `The whitelist for incoming notifications - i.e. ^http://.*-transformer-(pr|iw)-uk-.*\.svc\.ft\.com(:\d{2,5})?/content/[\w-]+.*$`,
		EnvVar: "WHITELIST",
	})
	logDebug := app.Bool(cli.BoolOpt{
		Name:   "logDebug",
		Value:  false,
		Desc:   "Flag to switch debug logging",
		EnvVar: "LOG_DEBUG",
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

	app.Before = func() {
		if err := checkMongoURLs(*mongos); err != nil {
			app.PrintHelp()
			log.Fatalf("Mongo connection is not set correctly: %s", err.Error())
		}
		_, err := regexp.Compile(*whitelist)
		if err != nil {
			app.PrintHelp()
			log.WithError(err).Fatal("Whitelist regex MUST compile!")
		}
	}

	app.Action = func() {
		if *logDebug {
			log.SetLevel(log.DebugLevel)
		} else {
			log.SetLevel(log.InfoLevel)
		}
		log.WithField("event", "service_started").WithField("service_name", *appName).Info("Service started")

		mongo := db.NewMongoDatabase(*mongos, 100)

		tr := &http.Transport{
			MaxIdleConnsPerHost: 128,
			Dial: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).Dial,
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
		if !(*isIncExportEnabled) {
			log.Warn("INCREMENTAL export is not enabled")
		} else {
			kafkaListener = prepareIncrementalExport(logDebug, consumerAddrs, consumerGroupID, topic, whitelist, exporter, delayForNotification, locker, maxGoRoutines)
			go kafkaListener.ConsumeMessages()
		}
		go func() {
			healthService := newHealthService(
				&healthConfig{
					appSystemCode: *appSystemCode,
					appName:       *appName,
					port:          *port,
					db:            mongo,
					enrichedContentFetcher: fetcher,
					s3Uploader:             uploader,
					queueHandler:           kafkaListener,
				})

			serveEndpoints(*appSystemCode, *appName, *port, web.NewRequestHandler(fullExporter, content.NewMongoInquirer(mongo), locker, *isIncExportEnabled, *contentRetrievalThrottle), healthService)
		}()

		waitForSignal()
		if *isIncExportEnabled {
			kafkaListener.StopConsumingMessages()
		}
		log.Info("Gracefully shut down")

	}

	err := app.Run(os.Args)
	if err != nil {
		log.Errorf("App could not start, error=[%s]\n", err)
		return
	}
}
func prepareIncrementalExport(logDebug *bool, consumerAddrs *string, consumerGroupID *string, topic *string, whitelist *string, exporter *content.Exporter, delayForNotification *int, locker *export.Locker, maxGoRoutines *int) *queue.KafkaListener {
	consumerConfig := kafka.DefaultConsumerConfig()
	consumerConfig.ChannelBufferSize = 10
	if *logDebug {
		sarama.Logger = standardlog.New(os.Stdout, "[sarama] ", standardlog.LstdFlags)
	} else {
		consumerConfig.Zookeeper.Logger = standardlog.New(ioutil.Discard, "", 0)
	}
	messageConsumer, err := kafka.NewConsumer(*consumerAddrs, *consumerGroupID, []string{*topic}, consumerConfig)
	if err != nil {
		log.WithError(err).Fatal("Cannot create Kafka client")
	}
	whitelistR, err := regexp.Compile(*whitelist)
	if err != nil {
		log.WithError(err).Fatal("Whitelist regex MUST compile!")
	}

	kafkaMessageHandler := queue.NewKafkaContentNotificationHandler(exporter, *delayForNotification)
	kafkaMessageMapper := queue.NewKafkaMessageMapper(whitelistR)
	kafkaListener := queue.NewKafkaListener(messageConsumer, kafkaMessageHandler, kafkaMessageMapper, locker, *maxGoRoutines)

	return kafkaListener
}

func serveEndpoints(appSystemCode string, appName string, port string, requestHandler *web.RequestHandler,
healthService *healthService) {

	serveMux := http.NewServeMux()

	hc := health.HealthCheck{SystemCode: appSystemCode, Name: appName, Description: appDescription, Checks: healthService.checks}
	serveMux.HandleFunc(healthPath, health.Handler(hc))
	serveMux.HandleFunc(status.GTGPath, status.NewGoodToGoHandler(healthService.gtgCheck))
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
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
}

func checkMongoURLs(providedMongoUrls string) error {
	if providedMongoUrls == "" {
		return errors.New("MongoDB urls are missing")
	}

	mongoUrls := strings.Split(providedMongoUrls, ",")

	for _, mongoUrl := range mongoUrls {
		host, port, err := net.SplitHostPort(mongoUrl)
		if err != nil {
			return fmt.Errorf("Cannot split MongoDB URL: %s into host and port. Error is: %s", mongoUrl, err.Error())
		}
		if host == "" || port == "" {
			return fmt.Errorf("One of the MongoDB URLs is incomplete: %s. It should have host and port.", mongoUrl)
		}
	}

	return nil
}
