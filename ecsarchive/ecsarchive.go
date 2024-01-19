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

	_ "github.com/lib/pq"
)

const (
	MANIFEST_FILENAME = "manifest.txt"
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
	errors     error
}

type ECSArchive struct {
	mu       sync.Mutex
	db       *sql.DB
	updater  *content.S3Updater
	archives map[string]archive
}

func NewECSAarchive(db *sql.DB, updater *content.S3Updater) *ECSArchive {
	return &ECSArchive{
		db:       db,
		updater:  updater,
		archives: make(map[string]archive),
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

func (ea *ECSArchive) OutputArchive(key string) (string, error) {
	ea.mu.Lock()
	defer ea.mu.Unlock()
	a, prs := ea.archives[key]
	if !prs {
		return "", errors.New("archive does not exist")
	}
	return fmt.Sprintf(`{"key": "%s", "state": "%s", "url":  "%s"}`, a.key, a.state, a.presignurl), nil
}

func (ea *ECSArchive) AddPresignUrlToArchive(key, url string) error {
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

func (ea *ECSArchive) GenerateArchiveS3(startDate, endDate, tid string) error {
	key := startDate + "-" + endDate + ".zip"
	ea.CreateArchive(key)

	articles, err := ea.getArticles(startDate, endDate)
	if err != nil {
		ea.DeleteArchive(key)
		return err
	}
	archive, err := ea.ExportArticlesZip(articles, startDate, endDate)
	if err != nil {
		ea.DeleteArchive(key)
		return err
	}

	err = ea.updater.UploadZip(archive, key, tid)
	if err != nil {
		return err
	}
	ea.ChangeArchiveState(key, "CREATED")

	pu, err := ea.updater.PresignURL(key, tid)
	if err != nil {
		return err
	}

	if err := ea.AddPresignUrlToArchive(key, pu.URL); err != nil {
		return err
	}

	return nil
}

func (ea *ECSArchive) ExportArticlesZip(articles <-chan Article, startDate, endDate string) (*bytes.Buffer, error) {
	buf := new(bytes.Buffer)
	writer := zip.NewWriter(buf)

	defer func(writer *zip.Writer) {
		if err := writer.Close(); err != nil {
			fmt.Printf("Failed to close writer with :%e", err)
		}
	}(writer)

	encodedArticle := new(bytes.Buffer)
	encoder := json.NewEncoder(encodedArticle)
	encoder.SetEscapeHTML(false)

	manifest_content := ""
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

		manifest_content += (fileName + "\n")
	}
	manifestWriter, err := writer.Create(MANIFEST_FILENAME)
	if err != nil {
		return nil, err
	}
	if _, err = manifestWriter.Write([]byte(manifest_content)); err != nil {
		return nil, err
	}
	return buf, nil
}

func (ea *ECSArchive) getArticles(startDate, endDate string) (chan Article, error) {
	dateRegex, err := regexp.Compile(`\d{4}-\d{2}-\d{2}`)
	if err != nil {
		return nil, err
	}
	if !dateRegex.MatchString(startDate) || !dateRegex.MatchString(endDate) {
		return nil, fmt.Errorf("dates must be in format yyyy-mm-dd")
	}

	if err = ea.db.Ping(); err != nil {
		return nil, err
	}

	rows, err := ea.db.Query(QUERY, endDate, startDate)
	if err != nil {
		return nil, err
	}

	ch := make(chan Article, 10)
	go iterateRows(rows, ch)
	return ch, nil
}

func iterateRows(rows *sql.Rows, ch chan<- Article) {
	for rows.Next() {
		article := &Article{
			Language: "en",
		}
		if err := rows.Scan(&article.UUID, &article.Body, &article.CanonicalURL, &article.DateCreated, &article.DateModified, &article.Authors); err != nil {
			fmt.Println("Failed to scan article with:")
			//panic(err)
		}

		transformedBody, err := bodytransformer.TransformBody(article.Body)
		if err != nil {
			fmt.Printf("Failed to transform body with:")
			//panic(err)
		}
		article.Body = transformedBody

		if article.CanonicalURL == "" {
			createdURL := fmt.Sprintf("https://www.ft.com/content/%s", article.UUID)
			article.CanonicalURL = createdURL
		}

		ch <- *article
	}
	if err := rows.Err(); err != nil {
		//panic(err)
	}
	rows.Close()
	close(ch)
}
