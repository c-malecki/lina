package apify

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type APIFY_ACTOR string
type ACTOR_STATUS string

const (
	SUCCEEDED ACTOR_STATUS = "SUCCEEDED"
	RUNNING   ACTOR_STATUS = "RUNNING"
)

const (
	LINKEDIN_PERSONS_ACTOR   APIFY_ACTOR = "apimaestro~linkedin-profile-batch-scraper-no-cookies-required"
	LINKEDIN_COMPANIES_ACTOR APIFY_ACTOR = "apimaestro~linkedin-company-detail"
	postRunActorAsync        string      = "https://api.apify.com/v2/acts/%s/runs"
	postRunActorSyncDataset  string      = "https://api.apify.com/v2/acts/%s/run-sync-get-dataset-items"
	getActorRun              string      = "https://api.apify.com/v2/actor-runs/%s"
	getActorRunLog           string      = "https://api.apify.com/v2/actor-runs/%s/log"
	getActorLastRun          string      = "https://api.apify.com/v2/acts/%s/runs/last"
	getActorLastRunDataset   string      = "https://api.apify.com/v2/acts/%s/runs/last/dataset/items?format=json"
	getDataset               string      = "https://api.apify.com/v2/datasets/%s/items?format=json"
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

/*
persons

const input = {
    "usernames": [
        "https://www.linkedin.com/in/neal-mohan",
    ],
    "includeEmail": false
};

companies
const input = {
    "identifier": [
        "https://www.linkedin.com/company/google/",
    ]
};
*/

func (c *ApifyClient) RunActor(actor APIFY_ACTOR, profileUrls []string) (RunResponse, error) {
	var run RunResponse

	input := make(map[string]interface{})
	if actor == LINKEDIN_PERSONS_ACTOR {
		input["usernames"] = profileUrls
	} else {
		input["identifier"] = profileUrls
	}

	payload, err := json.MarshalIndent(input, "", "   ")
	if err != nil {
		return run, err
	}

	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf(postRunActorAsync, actor), bytes.NewBuffer(payload))
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

func RunActorAndGetResults[T any](c *ApifyClient, actor APIFY_ACTOR, profileUrls []string, res []T) ([]T, error) {
	run, err := c.RunActor(actor, profileUrls)
	if err != nil {
		return nil, err
	}

	var datasetId string
	for {
		run, err = c.GetRun(run.Data.Id)
		if err != nil {
			return nil, err
		}

		if run.Data.Status == string(SUCCEEDED) {
			datasetId = run.Data.DefaultDatasetId
			break
		}

		time.Sleep(5 * time.Second)
	}

	dataset, err := c.GetDataset(datasetId)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(dataset, &res); err != nil {
		return nil, err
	}

	return res, nil
}
