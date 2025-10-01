package connection

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/c-malecki/lina/pipeline"
)

type ComparePhase struct {
	startTime time.Time
	endTime   time.Time
	pipeline  *ConnectionPipeline
	next      *RequestPhase
}

func (p *ComparePhase) Ended(err error) {
	p.endTime = time.Now()
	if err != nil {
		p.pipeline.End(pipeline.FAILED)
	}
}

func (p *ComparePhase) Start(ctx context.Context) {
	p.startTime = time.Now()
	p.pipeline.SetCurrent(p)

	if err := p.pipeline.dbw.SQLC.UpdateTmpConnectionPersonIDs(ctx); err != nil {
		p.Ended(err)
		return
	}

	newPersonUrls, err := p.pipeline.dbw.SQLC.SelectTmpConnectionsNoPersonIDs(ctx)
	if err != nil {
		p.Ended(err)
		return
	}

	addCt, err := p.pipeline.dbw.SQLC.CountConnectionsToAdd(ctx, p.pipeline.network.ID)
	if err != nil {
		p.Ended(err)
		return
	}

	removeCt, err := p.pipeline.dbw.SQLC.CountConnectionsToRemove(ctx, p.pipeline.network.ID)
	if err != nil {
		p.Ended(err)
		return
	}

	possibleNewCt := int64(len(newPersonUrls)) + addCt

	fmt.Printf("\n\n%d new connections will be added and %d connections will be removed\n", possibleNewCt, removeCt)
	fmt.Print("Do you wish to proceed? [Y/n] ")

	reader := bufio.NewReader(os.Stdin)
	opt, _ := reader.ReadString('\n')
	opt = strings.TrimSpace(opt)

	if opt == "n" {
		p.Ended(nil)
		return
	}

	p.next.Start(ctx)
}
