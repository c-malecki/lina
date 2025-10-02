package connection

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/c-malecki/lina/pipeline"
)

type TestComparePhase struct {
	startTime time.Time
	endTime   time.Time
	pipeline  *ConnectionPipeline
	next      pipeline.Phase
	test      *testing.T
}

func (p *TestComparePhase) Ended(err error) {
	p.endTime = time.Now()
	if err != nil {
		p.pipeline.End(pipeline.FAILED)
	}
}

func (p *TestComparePhase) Next() pipeline.Phase {
	return p.next
}

func (p *TestComparePhase) Start(ctx context.Context) {
	p.startTime = time.Now()
	p.pipeline.SetCurrent(p)
	fmt.Printf("\n=== compare phase ===\n")

	if err := p.pipeline.dbw.SQLC.UpdateTmpConnectionPersonIDs(ctx); err != nil {
		p.Ended(err)
		p.test.Fatal(err)
		return
	}

	newPersonUrls, err := p.pipeline.dbw.SQLC.SelectTmpConnectionsNoPersonIDs(ctx)
	if err != nil {
		p.Ended(err)
		p.test.Fatal(err)
		return
	}

	addCt, err := p.pipeline.dbw.SQLC.CountConnectionsToAdd(ctx, p.pipeline.network.ID)
	if err != nil {
		p.Ended(err)
		p.test.Fatal(err)
		return
	}

	removeCt, err := p.pipeline.dbw.SQLC.CountConnectionsToRemove(ctx, p.pipeline.network.ID)
	if err != nil {
		p.Ended(err)
		p.test.Fatal(err)
		return
	}

	possibleNewCt := int64(len(newPersonUrls)) + addCt

	fmt.Printf("\n%d new connections will be added", possibleNewCt)
	fmt.Printf("\n%d connections will be removed\n", removeCt)

	p.next.Start(ctx)
}
