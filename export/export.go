package export

import (
	"fmt"
	"sync"
	"time"

	"github.com/Financial-Times/content-exporter/content"
	"github.com/Financial-Times/go-logger/v2"
	"github.com/google/uuid"
)

type FullExporter struct {
	sync.RWMutex
	jobs                  map[string]*Job
	nrOfConcurrentWorkers int
	*content.Exporter
}

type State string

const (
	STARTING State = "Starting"
	RUNNING  State = "Running"
	FINISHED State = "Finished"
)

var ErrJobNotFound = fmt.Errorf("job not found")

type Job struct {
	lock                     *sync.RWMutex
	wg                       *sync.WaitGroup
	log                      *logger.UPPLogger
	nrWorker                 int
	contentRetrievalThrottle int
	isFullExport             bool

	ID           string   `json:"ID"`
	Count        int      `json:"Count,omitempty"`
	Progress     int      `json:"Progress,omitempty"`
	Failed       []string `json:"Failed,omitempty"`
	Status       State    `json:"Status"`
	ErrorMessage string   `json:"ErrorMessage,omitempty"`
}

func NewJob(nrWorker int, contentRetrievalThrottle int, isFullExport bool, log *logger.UPPLogger) *Job {
	return &Job{
		ID:                       uuid.New().String(),
		nrWorker:                 nrWorker,
		contentRetrievalThrottle: contentRetrievalThrottle,
		isFullExport:             isFullExport,
		log:                      log,
		lock:                     &sync.RWMutex{},
		wg:                       &sync.WaitGroup{},
		Status:                   STARTING,
	}
}

func NewFullExporter(nrOfWorkers int, exporter *content.Exporter) *FullExporter {
	return &FullExporter{
		jobs:                  make(map[string]*Job),
		nrOfConcurrentWorkers: nrOfWorkers,
		Exporter:              exporter,
	}
}

func (fe *FullExporter) GetRunningJobs() []Job {
	fe.RLock()
	defer fe.RUnlock()
	var jobs []Job
	for _, job := range fe.jobs {
		if job.Status == RUNNING {
			jobs = append(jobs, job.Copy())
		}
	}
	return jobs
}

func (fe *FullExporter) GetJob(jobID string) (Job, error) {
	fe.RLock()
	defer fe.RUnlock()
	job, ok := fe.jobs[jobID]
	if !ok {
		return Job{}, ErrJobNotFound
	}
	return job.Copy(), nil
}

func (fe *FullExporter) AddJob(job *Job) {
	if job != nil {
		fe.Lock()
		fe.jobs[job.ID] = job
		fe.Unlock()
	}
}

func (fe *FullExporter) GetWorkerCount() int {
	return fe.nrOfConcurrentWorkers
}

func (fe *FullExporter) IsFullExportRunning() bool {
	fe.RLock()
	defer fe.RUnlock()
	for _, job := range fe.jobs {
		if job.isFullExport && job.Status != FINISHED {
			return true
		}
	}
	return false
}

func (job *Job) Copy() Job {
	job.lock.Lock()
	defer job.lock.Unlock()
	return Job{
		Progress: job.Progress,
		Status:   job.Status,
		ID:       job.ID,
		Count:    job.Count,
		Failed:   job.Failed,
	}
}

func (job *Job) RunExport(tid string, docs chan *content.Stub, export func(string, *content.Stub) error) {
	job.log.Infof("Job started: %v", job.ID)
	job.Status = RUNNING
	workers := make(chan struct{}, job.nrWorker)
	for {
		doc, ok := <-docs
		if !ok {
			job.wg.Wait()
			job.Status = FINISHED
			job.log.Infof("Finished job %v with %v failure(s), progress: %v", job.ID, len(job.Failed), job.Progress)
			close(workers)
			return
		}

		workers <- struct{}{} // Will block until worker is available to span up new goroutines

		job.Progress++
		job.wg.Add(1)
		go func() {
			defer job.wg.Done()
			time.Sleep(time.Duration(job.contentRetrievalThrottle) * time.Millisecond)
			if err := export(tid, doc); err != nil {
				job.log.
					WithTransactionID(tid).
					WithUUID(doc.UUID).
					WithError(err).
					Error("Failed to process document")

				job.lock.Lock()
				job.Failed = append(job.Failed, doc.UUID)
				job.lock.Unlock()
			}
			<-workers
		}()
	}
}
