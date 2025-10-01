package connection

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/c-malecki/go-utils/database"
	"github.com/c-malecki/go-utils/parse/pstring"
	"github.com/c-malecki/lina/internal/model"
	"github.com/c-malecki/lina/pipeline"
)

const bom = "\uFEFF"

var csvheaders = [...]string{"first name", "last name", "url", "email address", "company", "position", "connected on"}

type CsvPhase struct {
	startTime time.Time
	endTime   time.Time
	pipeline  *ConnectionPipeline
	next      *ComparePhase
	csv       *os.File
}

func (p *CsvPhase) Ended(err error) {
	p.endTime = time.Now()
	if err != nil {
		p.pipeline.End(pipeline.FAILED)
	}
}

func (p *CsvPhase) Start(ctx context.Context) {
	p.startTime = time.Now()
	p.pipeline.SetCurrent(p)

	reader := csv.NewReader(p.csv)
	reader.FieldsPerRecord = -1

	var headers []string

	for {
		line, err := reader.Read()

		if err == io.EOF {
			p.Ended(errors.New("csv does not match the expected format"))
			return
		} else if err != nil {
			p.Ended(fmt.Errorf("error: read csv line %v", err))
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
		p.Ended(fmt.Errorf("csv headers length (%v) does not match expected length (%v)", len(headers), len(csvheaders)))
		return
	}

	for i, h := range headers {
		isSame := strings.EqualFold(h, csvheaders[i])
		if !isSame {
			p.Ended(fmt.Errorf("parsed header \"%s\" does not match expected header \"%s\"", h, csvheaders[i]))
			return
		}
	}

	liMap := make(map[string]struct{})

	validCt := 0
	invalidCt := 0

	fmt.Println("\nParsing CSV...")

	for {
		line, err := reader.Read()

		if err == io.EOF {
			break
		} else if err != nil {
			p.Ended(err)
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

	fmt.Printf("Valid CSV lines: %d\n", validCt)
	fmt.Printf("Invalid CSV lines: %d", invalidCt)

	if err := p.pipeline.dbw.SQLC.CreateTmpConnectionsTable(ctx); err != nil {
		p.Ended(err)
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
		return
	}

	p.next.Start(ctx)
}
