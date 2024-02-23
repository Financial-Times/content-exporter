package ecsarchive

import (
	"archive/zip"
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sync"

	bodytransformer "github.com/Financial-Times/cm-body-transformer"
	"github.com/Financial-Times/content-exporter/content"
	"github.com/Financial-Times/go-logger/v2"
)

const (
	ManifestFilename = "manifest.txt"
)

type Article struct {
	UUID         string
	Body         string
	Language     string
	Authors      string
	CanonicalURL string
	DateCreated  string
	DateModified string
}

type archive struct {
	state      string
	key        string
	presignurl string
}

type ECSArchive struct {
	running  chan bool
	mu       sync.Mutex
	db       *sql.DB
	updater  *content.S3Updater
	archives map[string]archive
}

func NewECSAarchive(db *sql.DB, updater *content.S3Updater, jobs int) *ECSArchive {
	return &ECSArchive{
		db:       db,
		updater:  updater,
		archives: make(map[string]archive),
		running:  make(chan bool, jobs),
	}
}

func (ea *ECSArchive) CreateArchive(key string) error {
	ea.mu.Lock()
	defer ea.mu.Unlock()
	_, prs := ea.archives[key]
	if prs {
		return errors.New("already created")
	}
	a := archive{state: "RUNNING", key: key}
	ea.archives[key] = a
	return nil
}

// Check the status of the archive
// IN PROGRESS or DONE
// We return error to indicate the archive does not exist
func (ea *ECSArchive) OutputArchive(key string) (string, error) {
	ea.mu.Lock()
	defer ea.mu.Unlock()
	a, prs := ea.archives[key]
	if !prs {
		return "", errors.New("archive does not exist")
	}
	return fmt.Sprintf(`{"key": "%s", "state": "%s", "url":  "%s"}`, a.key, a.state, a.presignurl), nil
}

func (ea *ECSArchive) AddPresignURLToArchive(key, url string) error {
	ea.mu.Lock()
	defer ea.mu.Unlock()
	a, prs := ea.archives[key]
	if !prs {
		return errors.New("archive does not exist")
	}
	a.presignurl = url
	ea.archives[key] = a
	return nil
}

func (ea *ECSArchive) ChangeArchiveState(key, state string) error {
	ea.mu.Lock()
	defer ea.mu.Unlock()
	a, prs := ea.archives[key]
	if !prs {
		return errors.New("archive does not exist")
	}
	a.state = state
	ea.archives[key] = a
	return nil
}

func (ea *ECSArchive) DeleteArchive(key string) {
	ea.mu.Lock()
	defer ea.mu.Unlock()
	delete(ea.archives, key)
}

// Run in go routine
func (ea *ECSArchive) GenerateArchiveS3(startDate, endDate, tid string, log *logger.LogEntry) {
	ea.running <- true
	defer func() {
		<-ea.running
	}()

	key := startDate + "-" + endDate + ".zip"
	if err := ea.CreateArchive(key); err != nil {
		log.WithError(err).Warn("CreateArchive error.")
		return
	}

	articles, errCh, err := ea.getArticles(startDate, endDate)
	if err != nil {
		ea.DeleteArchive(key)
		log.WithError(err).Warn("getArticles error.")
		return
	}
	go reportErrors(errCh, log)
	archive, err := ea.ExportArticlesZip(articles)
	if err != nil {
		ea.DeleteArchive(key)
		log.WithError(err).Warn("ExportArticlesZip error.")
		return
	}

	err = ea.updater.UploadZip(archive, key, tid)
	if err != nil {
		log.WithError(err).Warn("UploadZip error.")
		return
	}

	if err = ea.ChangeArchiveState(key, "CREATED"); err != nil {
		log.WithError(err).Warn("ChangeArchiveState error.")
		return
	}

	pu, err := ea.updater.PresignURL(key, tid)
	if err != nil {
		log.WithError(err).Warn("PresignURL error.")
		return
	}

	if err := ea.AddPresignURLToArchive(key, pu.URL); err != nil {
		log.WithError(err).Warn("AddPresignURLToArchive error.")
		return
	}

	PutBuffer(archive)
}

func (ea *ECSArchive) ExportArticlesZip(articles <-chan Article) (*bytes.Buffer, error) {
	buf := GetBuffer()
	writer := zip.NewWriter(buf)

	encodedArticle := new(bytes.Buffer)
	encoder := json.NewEncoder(encodedArticle)
	encoder.SetEscapeHTML(false)

	manifestContent := ""

	for article := range articles {
		fileName := fmt.Sprintf(
			"%s.json", article.UUID)
		fileWriter, err := writer.Create(fileName)
		if err != nil {
			return nil, err
		}

		if err = encoder.Encode(article); err != nil {
			return nil, err
		}

		if _, err = encodedArticle.WriteTo(fileWriter); err != nil {
			return nil, err
		}

		manifestContent += (fileName + "\n")
	}

	manifestWriter, err := writer.Create(ManifestFilename)
	if err != nil {
		return nil, err
	}
	if _, err = manifestWriter.Write([]byte(manifestContent)); err != nil {
		return nil, err
	}

	if err := writer.Close(); err != nil {
		return nil, err
	}

	return buf, nil
}

func (ea *ECSArchive) getArticles(startDate, endDate string) (chan Article, chan error, error) {
	dateRegex, err := regexp.Compile(`\d{4}-\d{2}-\d{2}`)
	if err != nil {
		return nil, nil, err
	}

	if !dateRegex.MatchString(startDate) || !dateRegex.MatchString(endDate) {
		return nil, nil, fmt.Errorf("dates must be in format yyyy-mm-dd")
	}

	if err = ea.db.Ping(); err != nil {
		return nil, nil, err
	}

	rows, err := ea.db.Query(QUERY, endDate, startDate)
	if err != nil {
		return nil, nil, err
	}

	ch := make(chan Article, 10)
	errCh := make(chan error)
	go iterateRows(rows, ch, errCh)
	return ch, errCh, nil
}

func iterateRows(rows *sql.Rows, ch chan<- Article, errCh chan<- error) {
	for rows.Next() {
		article := &Article{
			Language: "en",
		}
		if err := rows.Scan(&article.UUID, &article.Body, &article.CanonicalURL, &article.DateCreated, &article.DateModified, &article.Authors); err != nil {
			errCh <- fmt.Errorf("failied to scan article with: %w", err)
			break
		}

		transformedBody, err := bodytransformer.TransformBody(article.Body)
		if err != nil {
			errCh <- fmt.Errorf("failed to transform body with: %w", err)
			break
		}
		article.Body = transformedBody

		if article.CanonicalURL == "" {
			createdURL := fmt.Sprintf("https://www.ft.com/content/%s", article.UUID)
			article.CanonicalURL = createdURL
		}

		ch <- *article
	}
	if err := rows.Err(); err != nil {
		errCh <- err
	}
	rows.Close()
	close(ch)
	close(errCh)
}

func reportErrors(errCh <-chan error, log *logger.LogEntry) {
	for err := range errCh {
		log.WithError(err).Warn("Error in iterateRows.")
	}
}
