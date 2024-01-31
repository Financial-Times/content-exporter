package content

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

type Presignurl struct {
	URL string `json:"url"`
}

type updater interface {
	Upload(content []byte, tid, uuid, date string) error
	Delete(uuid, tid string) error
}

type S3Updater struct {
	apiClient           httpClient
	healthClient        httpClient
	writerAPIURL        string
	writerGenericAPIURL string
	presignerAPIURL     string
	writerHealthURL     string
}

func NewS3Updater(apiClient, healthClient httpClient, writerAPIURL, writerGenericAPIURL, presignerAPIURL, writerHealthURL string) *S3Updater {
	return &S3Updater{
		apiClient:           apiClient,
		healthClient:        healthClient,
		writerAPIURL:        writerAPIURL,
		writerGenericAPIURL: writerGenericAPIURL,
		presignerAPIURL:     presignerAPIURL,
		writerHealthURL:     writerHealthURL,
	}
}

func (u *S3Updater) Delete(uuid, tid string) error {
	req, err := http.NewRequest("DELETE", u.writerAPIURL+uuid, nil)
	if err != nil {
		return err
	}
	req.Header.Add("User-Agent", "UPP Content Exporter")
	req.Header.Add("X-Request-Id", tid)

	resp, err := u.apiClient.Do(req)
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

	req, err := http.NewRequest("PUT", u.writerAPIURL+uuid+"?date="+date, buf)
	if err != nil {
		return err
	}
	req.Header.Add("User-Agent", "UPP Content Exporter")
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("X-Request-Id", tid)

	resp, err := u.apiClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if !(resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated) {
		return fmt.Errorf("uploading content failed with unexpected status code: %d", resp.StatusCode)
	}

	return nil
}

func (u *S3Updater) UploadZip(buf *bytes.Buffer, key, tid string) error {
	// Delete archive from memory after upload
	req, err := http.NewRequest("PUT", u.writerGenericAPIURL+key, buf)
	if err != nil {
		return err
	}
	defer req.Body.Close()
	req.Header.Add("User-Agent", "UPP Content Exporter")
	req.Header.Add("Content-Type", "application/zip")
	req.Header.Add("X-Request-Id", tid)

	resp, err := u.apiClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if !(resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated) {
		return fmt.Errorf("uploading zip file failed with unexpected status code: %d", resp.StatusCode)
	}

	return nil
}

func (u *S3Updater) PresignURL(key, tid string) (*Presignurl, error) {
	var p Presignurl
	req, err := http.NewRequest("GET", u.presignerAPIURL+key, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("User-Agent", "UPP Content Exporter")
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("X-Request-Id", tid)
	resp, err := u.apiClient.Do(req)
	if err != nil {
		return nil, err
	}
	err = json.NewDecoder(resp.Body).Decode(&p)
	if err != nil {
		return nil, err
	}
	return &p, nil
}

func (u *S3Updater) CheckHealth() (string, error) {
	req, err := http.NewRequest("GET", u.writerHealthURL, nil)
	if err != nil {
		return "", err
	}

	resp, err := u.healthClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("GTG failed with unexpected status code: %d", resp.StatusCode)
	}
	return "S3 Writer is good to go.", nil
}
