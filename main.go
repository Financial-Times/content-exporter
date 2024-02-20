package main

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"sync"
	"syscall"
	"time"

	"github.com/Financial-Times/content-exporter/content"
	"github.com/Financial-Times/content-exporter/ecsarchive"
	"github.com/Financial-Times/content-exporter/export"
	"github.com/Financial-Times/content-exporter/mongo"
	"github.com/Financial-Times/content-exporter/queue"
	"github.com/Financial-Times/content-exporter/web"
	health "github.com/Financial-Times/go-fthealth/v1_1"
	"github.com/Financial-Times/go-logger/v2"
	"github.com/Financial-Times/http-handlers-go/httphandlers"
	"github.com/Financial-Times/kafka-client-go/v4"
	status "github.com/Financial-Times/service-status-go/httphandlers"
	"github.com/gorilla/mux"
	cli "github.com/jawher/mow.cli"
	"github.com/rcrowley/go-metrics"
	"github.com/sethgrid/pester"

	_ "github.com/lib/pq"
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
	ecsDBCred := app.String(cli.StringOpt{
		Name:   "ecsDBCred",
		Value:  "",
		Desc:   "ECS credentials",
		EnvVar: "ECS_DB_CRED",
	})
	ecsDBAddress := app.String(cli.StringOpt{
		Name:   "ecsDBAddress",
		Value:  "",
		Desc:   "ECS DB Address",
		EnvVar: "ECS_DB_ADDR",
	})
	dbAddress := app.String(cli.StringOpt{
		Name:   "dbAddress",
		Value:  "",
		Desc:   "Database address to connect to",
		EnvVar: "DB_CLUSTER_ADDRESS",
	})
	dbUsername := app.String(cli.StringOpt{
		Name:   "dbUsername",
		Value:  "",
		Desc:   "Username to connect to database",
		EnvVar: "DB_USERNAME",
	})
	dbPassword := app.String(cli.StringOpt{
		Name:   "dbPassword",
		Value:  "",
		Desc:   "Password to use to connect to database",
		EnvVar: "DB_PASSWORD",
	})

	dbTimeout := app.Int(cli.IntOpt{
		Name:   "dbTimeout",
		Desc:   "Mongo session connection timeout in seconds. (e.g. 60)",
		EnvVar: "DB_TIMEOUT",
		Value:  60,
	})
	dbName := app.String(cli.StringOpt{
		Name:   "dbName",
		Value:  "upp-store",
		Desc:   "Mongo database to read from",
		EnvVar: "DB_NAME",
	})
	dbCollection := app.String(cli.StringOpt{
		Name:   "dbCollection",
		Value:  "content",
		Desc:   "Mongo collection to read from",
		EnvVar: "DB_COLLECTION",
	})
	enrichedContentAPIURL := app.String(cli.StringOpt{
		Name:   "enrichedContentAPIURL",
		Value:  "http://localhost:8080/enrichedcontent/",
		Desc:   "API URL to enriched content endpoint",
		EnvVar: "ENRICHED_CONTENT_API_URL",
	})
	enrichedContentHealthURL := app.String(cli.StringOpt{
		Name:   "enrichedContentHealthURL",
		Value:  "http://localhost:8080/__gtg",
		Desc:   "Health URL to enriched content endpoint",
		EnvVar: "ENRICHED_CONTENT_HEALTH_URL",
	})
	s3WriterAPIURL := app.String(cli.StringOpt{
		Name:   "s3WriterAPIURL",
		Value:  "http://localhost:8080/content/",
		Desc:   "API URL to S3 writer endpoint",
		EnvVar: "S3_WRITER_API_URL",
	})
	s3WriterGenericAPIURL := app.String(cli.StringOpt{
		Name:   "s3WriterGenericAPIURL",
		Value:  "http://localhost:8080/generic/",
		Desc:   "API URL to S3 generic writer endpoint",
		EnvVar: "S3_WRITER_GENERIC_API_URL",
	})
	s3PresignerAPIURL := app.String(cli.StringOpt{
		Name:   "s3PresignerAPIURL",
		Value:  "http://localhost:8080/presigner/",
		Desc:   "API URL to S3 signer endpoint",
		EnvVar: "S3_PRESIGNER_API_URL",
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

	rangeInHours := app.Int(cli.IntOpt{
		Name:   "rangeInHours",
		Value:  2920,
		Desc:   "Restrict asking range for ECS Archive",
		EnvVar: "RANGE_IN_HOURS",
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
	kafkaClusterArn := app.String(cli.StringOpt{
		Name:   "kafkaClusterArn",
		Desc:   "Kafka cluster ARN",
		EnvVar: "KAFKA_CLUSTER_ARN",
	})
	allowedPublishUUIDs := app.Strings(cli.StringsOpt{
		Name:   "allowedPublishUUIDs",
		Value:  []string{},
		Desc:   `Comma-separated list of UUIDs`,
		EnvVar: "ALLOWED_PUBLISH_UUIDS",
	})

	log := logger.NewUPPLogger(serviceName, *logLevel)

	app.Before = func() {
		_ = regexp.MustCompile(*contentOriginAllowlist)
	}

	app.Action = func() {
		timeout := time.Duration(*dbTimeout) * time.Second
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		mongoClient, err := mongo.NewClient(ctx, *dbAddress, *dbUsername, *dbPassword, *dbName, *dbCollection, log)
		if err != nil {
			log.WithError(err).Fatal("Error establishing database connection")
		}

		// Open DB Connection to ecs db
		ecsDB, err := sql.Open(
			"postgres",
			fmt.Sprintf(
				"postgres://%s@%s",
				*ecsDBCred,
				*ecsDBAddress,
			),
		)
		defer func(ecsDB *sql.DB) {
			if err = ecsDB.Close(); err != nil {
				log.WithError(err).Fatalf("Failed to close database connection with :%e", err)
			}
		}(ecsDB)

		apiClient := newAPIClient()
		healthClient := newHealthClient()

		fetcher := content.NewEnrichedContentFetcher(apiClient, healthClient, *enrichedContentAPIURL, *enrichedContentHealthURL, *xPolicyHeaderValues, *authorization)
		uploader := content.NewS3Updater(apiClient, healthClient, *s3WriterAPIURL, *s3WriterGenericAPIURL, *s3PresignerAPIURL, *s3WriterHealthURL)

		ecsArchive := ecsarchive.NewECSAarchive(ecsDB, uploader, 1)

		exporter := content.NewExporter(fetcher, uploader)
		fullExporter := export.NewFullExporter(20, exporter)
		locker := export.NewLocker()
		var kafkaListener *queue.Listener

		if *kafkaClusterArn == "" {
			log.Fatalf("Could not load kafka cluster ARN")
		}

		if *isIncExportEnabled {
			kafkaListener, err = prepareIncrementalExport(
				log,
				consumerAddrs,
				consumerGroupID,
				topic,
				contentOriginAllowlist,
				*allowedContentTypes,
				exporter,
				delayForNotification,
				locker,
				maxGoRoutines,
				*kafkaClusterArn,
				*allowedPublishUUIDs,
			)

			if err != nil {
				log.WithError(err).Fatal("Failed to create consumer")
			}

			go kafkaListener.Start()
			defer kafkaListener.Stop()
		} else {
			log.Warn("INCREMENTAL export is not enabled")
		}

		hService := newHealthService(mongoClient, fetcher, uploader, kafkaListener, fullExporter)
		inquirer := mongo.NewInquirer(mongoClient, log)
		requestHandler := web.NewRequestHandler(fullExporter, inquirer, locker, *isIncExportEnabled, *contentRetrievalThrottle, log, ecsArchive, *rangeInHours)
		go serveEndpoints(*appSystemCode, *appName, *port, log, requestHandler, hService)

		log.
			WithField("event", "service_started").
			WithField("app-name", *appName).Info("Service started")

		waitForSignal()
		log.Info("Gracefully shut down")
	}

	err := app.Run(os.Args)
	if err != nil {
		log.WithError(err).Fatal("App could not start")
	}
}

func newAPIClient() *pester.Client {
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

	return client
}

func newHealthClient() *http.Client {
	tr := &http.Transport{
		MaxIdleConnsPerHost: 10,
		DialContext: (&net.Dialer{
			Timeout:   3 * time.Second,
			KeepAlive: 3 * time.Second,
		}).DialContext,
	}

	return &http.Client{
		Transport: tr,
		Timeout:   3 * time.Second,
	}
}

func prepareIncrementalExport(
	log *logger.UPPLogger,
	consumerAddrs, consumerGroupID, topic, contentOriginAllowlist *string,
	allowedContentTypes []string,
	exporter *content.Exporter,
	delayForNotification *int,
	locker *export.Locker,
	maxGoRoutines *int,
	kafkaClusterArn string,
	allowedPublishUUIDs []string,
) (*queue.Listener, error) {
	config := kafka.ConsumerConfig{
		ClusterArn:              &kafkaClusterArn,
		BrokersConnectionString: *consumerAddrs,
		ConsumerGroup:           *consumerGroupID,
	}
	topics := []*kafka.Topic{
		kafka.NewTopic(*topic),
	}
	messageConsumer, err := kafka.NewConsumer(config, topics, log)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	contentOriginAllowListRegex := regexp.MustCompile(*contentOriginAllowlist)

	messageHandler := queue.NewNotificationHandler(exporter, *delayForNotification)
	messageMapper := queue.NewMessageMapper(contentOriginAllowListRegex, allowedContentTypes, allowedPublishUUIDs)
	listener := queue.NewListener(messageConsumer, messageHandler, messageMapper, locker, *maxGoRoutines, log)

	return listener, nil
}

func serveEndpoints(appSystemCode, appName, port string, log *logger.UPPLogger, requestHandler *web.RequestHandler, healthService *healthService) {
	serveMux := http.NewServeMux()

	hc := health.HealthCheck{SystemCode: appSystemCode, Name: appName, Description: appDescription, Checks: healthService.healthChecks}
	serveMux.HandleFunc(healthPath, health.Handler(hc))
	serveMux.HandleFunc(status.GTGPath, status.NewGoodToGoHandler(healthService.GTG))
	serveMux.HandleFunc(status.BuildInfoPath, status.BuildInfoHandler)

	servicesRouter := mux.NewRouter()
	servicesRouter.HandleFunc("/export", requestHandler.Export).Methods(http.MethodPost)
	servicesRouter.HandleFunc("/jobs/{jobID}", requestHandler.GetJob).Methods(http.MethodGet)
	servicesRouter.HandleFunc("/jobs", requestHandler.GetRunningJobs).Methods(http.MethodGet)
	servicesRouter.HandleFunc("/ecsarchive/{startDate}/{endDate}", requestHandler.GenerateArticlesZipS3).Methods(http.MethodGet)

	var monitoringRouter http.Handler = servicesRouter
	monitoringRouter = httphandlers.TransactionAwareRequestLoggingHandler(log, monitoringRouter)
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
