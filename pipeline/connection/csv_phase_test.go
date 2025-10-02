package connection

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/c-malecki/go-utils/database"
	"github.com/c-malecki/go-utils/parse/pstring"
	"github.com/c-malecki/lina/internal/model"
	"github.com/c-malecki/lina/pipeline"
)

type TestCsvPhase struct {
	startTime time.Time
	endTime   time.Time
	pipeline  *ConnectionPipeline
	next      pipeline.Phase
	csv       *os.File
	test      *testing.T
}

func (p *TestCsvPhase) Ended(err error) {
	p.endTime = time.Now()
	if err != nil {
		p.pipeline.End(pipeline.FAILED)
	}
}

func (p *TestCsvPhase) Next() pipeline.Phase {
	return p.next
}

func (p *TestCsvPhase) Start(ctx context.Context) {
	p.startTime = time.Now()
	p.pipeline.SetCurrent(p)
	fmt.Printf("\n=== csv phase ===\n")

	reader := csv.NewReader(p.csv)
	reader.FieldsPerRecord = -1
	defer p.csv.Close()

	var headers []string

	for {
		line, err := reader.Read()

		if err == io.EOF {
			err := errors.New("csv does not match the expected format")
			p.Ended(err)
			p.test.Fatal(err)
			return
		} else if err != nil {
			err := fmt.Errorf("error: read csv line %v", err)
			p.Ended(err)
			p.test.Fatal(err)
			return
		}

		firstCol := line[0]

		if hasBom := strings.HasPrefix(firstCol, bom); hasBom {
			firstCol = strings.TrimPrefix(firstCol, bom)
			line[0] = firstCol
		}

		if isSame := strings.EqualFold(firstCol, "first name"); isSame {
			headers = line
			break
		}
	}

	if len(headers) != len(csvheaders) {
		err := fmt.Errorf("csv headers length (%v) does not match expected length (%v)", len(headers), len(csvheaders))
		p.Ended(err)
		p.test.Fatal(err)
		return
	}

	for i, h := range headers {
		isSame := strings.EqualFold(h, csvheaders[i])
		if !isSame {
			err := fmt.Errorf("parsed header \"%s\" does not match expected header \"%s\"", h, csvheaders[i])
			p.Ended(err)
			p.test.Fatal(err)
			return
		}
	}

	liMap := make(map[string]struct{})

	validCt := 0
	invalidCt := 0

	fmt.Println("\nparsing csv...")

	for {
		line, err := reader.Read()

		if err == io.EOF {
			break
		} else if err != nil {
			p.Ended(err)
			p.test.Fatal(err)
			return
		}

		li, err := pstring.ExtractPersonLinkedin(line[2])
		if err != nil {
			invalidCt += 1
			continue
		}

		if _, ok := liMap[li]; !ok {
			liMap[li] = struct{}{}
		}
		validCt += 1
	}

	var linkedins []model.InsertTmpConnectionParams
	for k := range liMap {
		linkedins = append(linkedins, model.InsertTmpConnectionParams{
			NetworkID:  p.pipeline.network.ID,
			ProfileUrl: k,
		})
	}

	fmt.Printf("valid rows: %d\n", validCt)
	fmt.Printf("invalid rows: %d", invalidCt)

	if err := p.pipeline.dbw.SQLC.CreateTmpConnectionsTable(ctx); err != nil {
		p.Ended(err)
		p.test.Fatal(err)
		return
	}

	if _, err := database.BatchInsert(ctx, p.pipeline.dbw.DB, database.BatchInsertDesc[model.InsertTmpConnectionParams]{
		Query: model.InsertTmpConnection,
		Items: linkedins,
		ExtractFn: func(itcp model.InsertTmpConnectionParams) []interface{} {
			return []interface{}{
				itcp.NetworkID,
				itcp.ProfileUrl,
			}
		},
	}); err != nil {
		p.Ended(err)
		p.test.Fatal(err)
		return
	}

	p.next.Start(ctx)
}
