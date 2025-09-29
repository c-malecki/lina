package apify

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

type ACTOR_STATUS string

const (
	SUCCEEDED ACTOR_STATUS = "SUCCEEDED"
	RUNNING   ACTOR_STATUS = "RUNNING"
)

const (
	LINKEDIN_PERSONS_ACTOR   string = "apimaestro~linkedin-profile-batch-scraper-no-cookies-required"
	LINKEDIN_COMPANIES_ACTOR string = "apimaestro~linkedin-company-detail"
	postRunActorAsync        string = "https://api.apify.com/v2/acts/%s/runs"
	postRunActorSyncDataset  string = "https://api.apify.com/v2/acts/%s/run-sync-get-dataset-items"
	getActorRun              string = "https://api.apify.com/v2/actor-runs/%s"
	getActorRunLog           string = "https://api.apify.com/v2/actor-runs/%s/log"
	getActorLastRun          string = "https://api.apify.com/v2/acts/%s/runs/last"
	getActorLastRunDataset   string = "https://api.apify.com/v2/acts/%s/runs/last/dataset/items?format=json"
	getDataset               string = "https://api.apify.com/v2/datasets/%s/items?format=json"
)

type ApifyClient struct {
	client *http.Client
	token  string
}

func NewApifyClient(token string) *ApifyClient {
	return &ApifyClient{
		client: http.DefaultClient,
		token:  token,
	}
}

func (c *ApifyClient) RunActor(actor string) (RunResponse, error) {
	var run RunResponse

	payload := strings.NewReader(`{}`)
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf(postRunActorAsync, actor), payload)
	if err != nil {
		return run, err
	}

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Accept", "application/json")
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", c.token))

	res, err := c.client.Do(req)
	if err != nil {
		return run, err
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return run, err
	}

	if err := json.Unmarshal(body, &run); err != nil {
		return run, err
	}

	return run, nil
}

func (c *ApifyClient) GetRun(runId string) (RunResponse, error) {
	var run RunResponse

	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf(getActorRun, runId), nil)
	if err != nil {
		return run, err
	}

	req.Header.Add("Accept", "application/json")
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", c.token))

	res, err := c.client.Do(req)
	if err != nil {
		return run, err
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return run, err
	}

	if err := json.Unmarshal(body, &run); err != nil {
		return run, err
	}

	return run, nil
}

func (c *ApifyClient) GetDataset(datasetId string) ([]byte, error) {
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf(getDataset, datasetId), nil)
	if err != nil {
		return nil, err
	}

	req.Header.Add("Accept", "application/json")
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", c.token))

	res, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	return body, nil
}
