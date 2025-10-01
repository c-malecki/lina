package connection

import (
	"context"
	"time"

	"github.com/c-malecki/lina/pipeline"
)

type NetworkPhase struct {
	startTime time.Time
	endTime   time.Time
	pipeline  *ConnectionPipeline
}

func (p *NetworkPhase) Ended(err error) {
	p.endTime = time.Now()
	if err != nil {
		p.pipeline.End(pipeline.FAILED)
	}
}

func (p *NetworkPhase) Start(ctx context.Context) {
	p.startTime = time.Now()
	p.pipeline.SetCurrent(p)

	if err := p.pipeline.dbw.SQLC.InsertNewConnectionsFromTmp(ctx, p.pipeline.network.ID); err != nil {
		p.Ended(err)
		return
	}

	p.Ended(nil)
}
