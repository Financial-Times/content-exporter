package web

import (
	"encoding/json"
	"fmt"
	"github.com/Financial-Times/content-exporter/content"
	"github.com/Financial-Times/content-exporter/export"
	"github.com/Financial-Times/transactionid-utils-go"
	"github.com/gorilla/mux"
	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
)

type RequestHandler struct {
	FullExporter             *export.Service
	Inquirer                 content.Inquirer
	ContentRetrievalThrottle int
	*export.Locker
	IsIncExportEnabled       bool
}

func NewRequestHandler(fullExporter *export.Service, inquirer content.Inquirer, locker *export.Locker, isIncExportEnabled bool, contentRetrievalThrottle int) *RequestHandler {
	return &RequestHandler{
		FullExporter:       fullExporter,
		Inquirer:           inquirer,
		Locker:             locker,
		IsIncExportEnabled: isIncExportEnabled,
		ContentRetrievalThrottle:contentRetrievalThrottle,

	}
}

func (handler *RequestHandler) Export(writer http.ResponseWriter, request *http.Request) {
	defer request.Body.Close()

	tid := transactionidutils.GetTransactionIDFromRequest(request)

	jobs := handler.FullExporter.GetRunningJobs()
	if len(jobs) > 0 {
		http.Error(writer, "There are already running export jobs. Please wait them to finish", http.StatusBadRequest)
		return
	}

	if handler.IsIncExportEnabled {
		select {
		case handler.Locker.Locked <- true:
			log.Info("Lock initiated")
		case <-time.After(time.Second * 3):
			msg := "Lock initiation timed out"
			log.Infof(msg)
			http.Error(writer, msg, http.StatusServiceUnavailable)
			return
		}

		select {
		case <-handler.Locker.Acked:
			log.Info("Locker acquired")
		case <-time.After(time.Second * 20):
			msg := "Stopping kafka consumption timed out"
			log.Infof(msg)
			http.Error(writer, msg, http.StatusServiceUnavailable)
			return
		}
	}
	candidates := getCandidateUUIDs(request)

	jobID := uuid.New()
	job := &export.Job{ID: jobID, NrWorker: handler.FullExporter.NrOfConcurrentWorkers, Status: export.STARTING, ContentRetrievalThrottle:handler.ContentRetrievalThrottle}
	handler.FullExporter.AddJob(job)

	go func() {
		if handler.IsIncExportEnabled {
			defer func() {
				log.Info("Locker released")
				handler.Locker.Locked <- false
			}()
		}
		log.Infoln("Calling mongo")
		docs, err, count := handler.Inquirer.Inquire("content", candidates)
		if err != nil {
			msg := fmt.Sprintf(`Failed to read IDs from mongo for %v! "%v"`, "content", err.Error())
			log.Info(msg)
			job.ErrorMessage = msg
			job.Status = export.FINISHED
			return
		}
		log.Infof("Nr of UUIDs found: %v", count)
		job.DocIds = docs
		job.Count = count

		job.RunFullExport(tid, handler.FullExporter.HandleContent)
	}()

	writer.WriteHeader(http.StatusAccepted)
	writer.Header().Add("Content-Type", "application/json")

	err := json.NewEncoder(writer).Encode(job)
	if err != nil {
		msg := fmt.Sprintf(`Failed to write job %v to response writer: "%v"`, job.ID, err)
		log.Warn(msg)
		fmt.Fprintf(writer, "{\"ID\": \"%v\"}", job.ID)
		return
	}
}

func getCandidateUUIDs(request *http.Request) (candidates []string) {
	var result map[string]interface{}
	body, err := ioutil.ReadAll(request.Body)
	if err != nil {
		log.Debugf("No valid POST body found, thus no candidate ids to export. Parsing error: %v", err)
		return
	}

	if err = json.Unmarshal(body, &result); err != nil {
		log.Debugf("No valid json body found, thus no candidate ids to export. Parsing error: %v", err)
		return
	}
	log.Infof("DEBUG Parsing request body: %v", result)
	ids, ok := result["ids"]
	if !ok {
		log.Infof("No ids field found in json body, thus no candidate ids to export.")
		return
	}
	idsString, ok := ids.(string)
	if ok {
		candidates = strings.Split(idsString, " ")
	} else {
		log.Infof("The ids field found in json body is not a string as expected.")
	}

	return
}

func (handler *RequestHandler) GetJob(writer http.ResponseWriter, request *http.Request) {
	defer request.Body.Close()

	vars := mux.Vars(request)
	jobID := vars["jobID"]

	writer.Header().Add("Content-Type", "application/json")

	job, err := handler.FullExporter.GetJob(jobID)
	if err != nil {
		msg := fmt.Sprintf(`{"message":"%v"}`, err)
		log.Info(msg)
		http.Error(writer, msg, http.StatusNotFound)
		return
	}

	err = json.NewEncoder(writer).Encode(job)
	if err != nil {
		msg := fmt.Sprintf(`Failed to write job %v to response writer: "%v"`, job.ID, err)
		log.Warn(msg)
		fmt.Fprintf(writer, "{\"ID\": \"%v\"}", job.ID)
		return
	}
}

func (handler *RequestHandler) GetRunningJobs(writer http.ResponseWriter, request *http.Request) {
	defer request.Body.Close()

	writer.Header().Add("Content-Type", "application/json")

	jobs := handler.FullExporter.GetRunningJobs()

	err := json.NewEncoder(writer).Encode(jobs)
	if err != nil {
		msg := fmt.Sprintf(`Failed to get running jobs: "%v"`, err)
		log.Warn(msg)
		fmt.Fprintf(writer, "{\"Jobs\": \"%v\"}", jobs)
		return
	}
}
