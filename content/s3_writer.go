package content

import (
	"bytes"
	"fmt"
	"net/http"
)

const s3WriterPath = "/content/"

type updater interface {
	Upload(content []byte, tid, uuid, date string) error
	Delete(uuid, tid string) error
}

type S3Updater struct {
	client          httpClient
	writerBaseURL   string
	writerHealthURL string
}

func NewS3Updater(client httpClient, writerBaseURL string, writerHealthURL string) *S3Updater {
	return &S3Updater{
		client:          client,
		writerBaseURL:   writerBaseURL,
		writerHealthURL: writerHealthURL,
	}
}

func (u *S3Updater) Delete(uuid, tid string) error {
	req, err := http.NewRequest("DELETE", u.writerBaseURL+s3WriterPath+uuid, nil)
	if err != nil {
		return err
	}
	req.Header.Add("User-Agent", "UPP Content Exporter")
	req.Header.Add("X-Request-Id", tid)

	resp, err := u.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("deleting content failed with unexpected status code: %d", resp.StatusCode)
	}

	return nil
}

func (u *S3Updater) Upload(content []byte, tid, uuid, date string) error {
	buf := new(bytes.Buffer)
	_, err := buf.Write(content)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("PUT", u.writerBaseURL+s3WriterPath+uuid+"?date="+date, buf)
	if err != nil {
		return err
	}
	req.Header.Add("User-Agent", "UPP Content Exporter")
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("X-Request-Id", tid)

	resp, err := u.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if !(resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated) {
		return fmt.Errorf("uploading content failed with unexpected status code: %d", resp.StatusCode)
	}

	return nil
}

func (u *S3Updater) CheckHealth(client httpClient) (string, error) {
	req, err := http.NewRequest("GET", u.writerHealthURL, nil)
	if err != nil {
		return "", err
	}

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("GTG failed with unexpected status code: %d", resp.StatusCode)
	}
	return "S3 Writer is good to go.", nil
}
