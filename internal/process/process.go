package process

type PROCESS_NAME string

const (
	UPDATE_CONNECTIONS PROCESS_NAME = "update_connections"
)

type SUBPROCESS_NAME string

const (
	APIFY_PERSONS   SUBPROCESS_NAME = "apify_persons"
	APIFY_COMPANIES SUBPROCESS_NAME = "apify_companies"
)

type ACTION_STATUS int

const (
	FAILED    ACTION_STATUS = 0
	SUCCEEDED ACTION_STATUS = 1
	RUNNING   ACTION_STATUS = 2
)

func (ps ACTION_STATUS) String() string {
	var status string
	switch ps {
	case FAILED:
		status = "failed"
	case SUCCEEDED:
		status = "succeeded"
	case RUNNING:
		status = "running"
	}
	return status
}

type Subprocess struct {
	Name   PROCESS_NAME
	Status ACTION_STATUS
}

type Process struct {
	Name       PROCESS_NAME
	Status     ACTION_STATUS
	Subprocess Subprocess
}
