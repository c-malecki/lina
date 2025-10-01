package connection

import (
	"context"
	"fmt"
	"time"

	"github.com/c-malecki/lina/pipeline"
)

type DataPhase struct {
	startTime time.Time
	endTime   time.Time
	pipeline  *ConnectionPipeline
	next      *NetworkPhase
}

func (p *DataPhase) Ended(err error) {
	p.endTime = time.Now()
	if err != nil {
		p.pipeline.End(pipeline.FAILED)
	}
}

func (p *DataPhase) Start(ctx context.Context) {
	p.startTime = time.Now()
	p.pipeline.SetCurrent(p)

	if err := p.InsertDatasetDataFromTmp(ctx); err != nil {
		p.Ended(err)
		return
	}

	if err := p.InsertOrganizationDataFromTmp(ctx); err != nil {
		p.Ended(err)
		return
	}

	if err := p.InsertPersonDataFromTmp(ctx); err != nil {
		p.Ended(err)
		return
	}
}

func (p *DataPhase) InsertDatasetDataFromTmp(ctx context.Context) error {
	tx, err := p.pipeline.dbw.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("BeginTx %w", err)
	}
	qtx := p.pipeline.dbw.SQLC.WithTx(tx)

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

func (p *DataPhase) InsertOrganizationDataFromTmp(ctx context.Context) error {
	tx, err := p.pipeline.dbw.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("BeginTx %w", err)
	}
	qtx := p.pipeline.dbw.SQLC.WithTx(tx)

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

func (p *DataPhase) InsertPersonDataFromTmp(ctx context.Context) error {
	tx, err := p.pipeline.dbw.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("BeginTx %w", err)
	}
	qtx := p.pipeline.dbw.SQLC.WithTx(tx)

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
