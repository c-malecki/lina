package connection

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/c-malecki/go-utils/gen"
	"github.com/c-malecki/go-utils/parse/pstring"
	"github.com/c-malecki/go-utils/path"
	"github.com/c-malecki/lina/internal/model"
	"github.com/c-malecki/lina/pipeline"
)

type TestNetworkPhase struct {
	startTime time.Time
	endTime   time.Time
	pipeline  *ConnectionPipeline
	test      *testing.T
}

func (p *TestNetworkPhase) Ended(err error) {
	p.endTime = time.Now()
	if err != nil {
		p.pipeline.End(pipeline.FAILED)
	}
}

func (p *TestNetworkPhase) Next() pipeline.Phase {
	return nil
}

func (p *TestNetworkPhase) Start(ctx context.Context) {
	p.startTime = time.Now()
	p.pipeline.SetCurrent(p)
	fmt.Printf("\n=== network phase ===\n")

	if err := p.pipeline.dbw.SQLC.UpdateTmpConnectionPersonIDs(ctx); err != nil {
		p.test.Fatalf("UpdateTmpConnectionPersonIDs %v", err)
	}

	dir, err := path.FindProjectRoot()
	if err != nil {
		p.test.Fatalf("path.FindProjectRoot %v", err)
	}

	if cons, _ := p.pipeline.dbw.SQLC.SelectTestTmpConnections(ctx); len(cons) > 0 {
		if err := gen.GenOutputCsv(
			filepath.Join(dir, "dev", "output", "tmp_cons.csv"),
			[]string{"id", "network_id", "profile_url", "person_id"},
			cons,
			func(t model.TmpConnections) []string {
				var row []string
				row = append(row,
					pstring.SafeNumPtrToStr(&t.ID),
					pstring.SafeNumPtrToStr(&t.NetworkID),
					t.ProfileUrl,
					pstring.SafeNumPtrToStr(t.PersonID),
				)
				return row
			}); err != nil {
			p.test.Fatalf("createCsv tmp_cons.csv %v", err)
		}
	}

	if err := p.pipeline.dbw.SQLC.InsertNewConnectionsFromTmp(ctx); err != nil {
		p.Ended(err)
		p.test.Fatal(err)
		return
	}

	if err := p.pipeline.dbw.SQLC.DeleteConnectionsNotInTmp(ctx, p.pipeline.network.ID); err != nil {
		p.Ended(err)
		p.test.Fatal(err)
		return
	}

	if ct, err := p.pipeline.dbw.SQLC.CountConnectionsByNetworkID(ctx, p.pipeline.network.ID); err == nil {
		fmt.Printf("%d connections\n", ct)
	}

	p.Ended(nil)
}
