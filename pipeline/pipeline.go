package pipeline

import (
	"context"
)

type PIPELINE_STATUS int

const (
	FAILED    PIPELINE_STATUS = 0
	SUCCEEDED PIPELINE_STATUS = 1
	RUNNING   PIPELINE_STATUS = 2
	// UNINITIATED PIPELINE_STATUS = 3
)

func (s PIPELINE_STATUS) String() string {
	var status string
	switch s {
	case FAILED:
		status = "failed"
	case SUCCEEDED:
		status = "succeeded"
	case RUNNING:
		status = "running"
		// case UNINITIATED:
		// 	status = "uninitiated"
	}
	return status
}

type Phase interface {
	Start(ctx context.Context)
	Ended(err error)
	Next() Phase
}

type PipelinePhases map[string]Phase

type Pipeline interface {
	GetCurrent() Phase
	SetCurrent(Phase)
	End(PIPELINE_STATUS)
}
