package connection

import (
	"context"
	"fmt"
	"time"

	"github.com/c-malecki/lina/internal/dbw"
	"github.com/c-malecki/lina/pipeline"
)

type DataPhase struct {
	startTime time.Time
	endTime   time.Time
	pipeline  *ConnectionPipeline
	next      pipeline.Phase
}

func (p *DataPhase) Ended(err error) {
	p.endTime = time.Now()
	if err != nil {
		p.pipeline.End(pipeline.FAILED)
	}
}

func (p *DataPhase) Next() pipeline.Phase {
	return p.next
}

func (p *DataPhase) Start(ctx context.Context) {
	p.startTime = time.Now()
	p.pipeline.SetCurrent(p)

	if err := insertDatasetDataFromTmp(ctx, p.pipeline.dbw); err != nil {
		p.Ended(err)
		return
	}

	if err := insertOrganizationDataFromTmp(ctx, p.pipeline.dbw); err != nil {
		p.Ended(err)
		return
	}

	if err := insertPersonDataFromTmp(ctx, p.pipeline.dbw); err != nil {
		p.Ended(err)
		return
	}

	p.next.Start(ctx)
}

func insertDatasetDataFromTmp(ctx context.Context, dbw *dbw.DBW) error {
	tx, err := dbw.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("BeginTx %w", err)
	}
	qtx := dbw.SQLC.WithTx(tx)

	err = qtx.InsertDatasetDegreesFromTmp(ctx)
	if err != nil {
		return fmt.Errorf("InsertDatasetDegreesFromTmp %w", err)
	}

	err = qtx.InsertDatasetIndustriesFromTmp(ctx)
	if err != nil {
		return fmt.Errorf("InsertDatasetIndustriesFromTmp %w", err)
	}

	err = qtx.InsertDatasetLocationsFromTmp(ctx)
	if err != nil {
		return fmt.Errorf("InsertDatasetLocationsFromTmp %w", err)
	}

	err = qtx.InsertDatasetSkillsFromTmp(ctx)
	if err != nil {
		return fmt.Errorf("InsertDatasetSkillsFromTmp %w", err)
	}

	err = qtx.InsertDatasetSpecialtiesFromTmp(ctx)
	if err != nil {
		return fmt.Errorf("InsertDatasetSpecialtiesFromTmp %w", err)
	}

	err = qtx.InsertDatasetStudyFieldsFromTmp(ctx)
	if err != nil {
		return fmt.Errorf("InsertDatasetStudyFieldsFromTmp %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("tx.Commit %w", err)
	}

	return nil
}

func insertOrganizationDataFromTmp(ctx context.Context, dbw *dbw.DBW) error {
	tx, err := dbw.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("BeginTx %w", err)
	}
	qtx := dbw.SQLC.WithTx(tx)

	err = qtx.InsertOrganizationsFromTmp(ctx)
	if err != nil {
		return fmt.Errorf("InsertOrganizationsFromTmp %w", err)
	}

	err = qtx.InsertOrganizationLocationsFromTmp(ctx)
	if err != nil {
		return fmt.Errorf("InsertOrganizationLocationsFromTmp %w", err)
	}

	err = qtx.InsertOrganizationIndustriesFromTmp(ctx)
	if err != nil {
		return fmt.Errorf("InsertOrganizationIndustriesFromTmp %w", err)
	}

	err = qtx.InsertOrganizationSpecialtiesFromTmp(ctx)
	if err != nil {
		return fmt.Errorf("InsertOrganizationSpecialtiesFromTmp %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("tx.Commit %w", err)
	}

	return nil
}

func insertPersonDataFromTmp(ctx context.Context, dbw *dbw.DBW) error {
	tx, err := dbw.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("BeginTx %w", err)
	}
	qtx := dbw.SQLC.WithTx(tx)

	err = qtx.InsertPersonsFromTmp(ctx)
	if err != nil {
		return fmt.Errorf("InsertPersonsFromTmp %w", err)
	}

	err = qtx.InsertPersonSkillsFromTmp(ctx)
	if err != nil {
		return fmt.Errorf("InsertPersonSkillsFromTmp %w", err)
	}

	err = qtx.InsertExperiencesFromTmp(ctx)
	if err != nil {
		return fmt.Errorf("InsertExperiencesFromTmp %w", err)
	}

	err = qtx.InsertEducationsFromTmp(ctx)
	if err != nil {
		return fmt.Errorf("InsertEducationsFromTmp %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("tx.Commit %w", err)
	}

	return nil
}
