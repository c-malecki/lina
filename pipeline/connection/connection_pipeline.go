package connection

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/c-malecki/lina/internal/dbw"
	"github.com/c-malecki/lina/internal/model"
	"github.com/c-malecki/lina/pipeline"
)

type ConnectionPipeline struct {
	user      *model.Users
	network   *model.Networks
	dbw       *dbw.DBW
	startTime time.Time
	endTime   time.Time
	current   pipeline.Phase
	status    pipeline.PIPELINE_STATUS
}

func (p *ConnectionPipeline) GetCurrent() pipeline.Phase {
	return p.current
}

func (p *ConnectionPipeline) SetCurrent(phase pipeline.Phase) {
	p.current = phase
}

func (p *ConnectionPipeline) End(status pipeline.PIPELINE_STATUS) {
	p.endTime = time.Now()
	p.status = status
}

func InitConnectionPipeline(user *model.Users, network *model.Networks, dbw *dbw.DBW) {
	var valid bool
	fmt.Print("\nPath to LinkedIn connections.csv: ")

	var f *os.File

	for !valid {
		reader := bufio.NewReader(os.Stdin)
		path, _ := reader.ReadString('\n')
		path = strings.TrimSpace(path)

		file, err := os.Open(path)
		if err != nil {
			fmt.Printf("%v", err)
			continue
		}
		f = file

		valid = true
	}

	p := &ConnectionPipeline{
		user:      user,
		network:   network,
		dbw:       dbw,
		startTime: time.Now(),
		status:    pipeline.RUNNING,
	}

	networkp := &NetworkPhase{}
	data := &DataPhase{pipeline: p, next: networkp}
	request := &RequestPhase{pipeline: p, next: data}
	compare := &ComparePhase{pipeline: p, next: request}
	csv := &CsvPhase{pipeline: p, next: compare, csv: f}

	csv.Start(context.Background())
}
