package network

import (
	"context"
	"fmt"
	"strings"

	"github.com/c-malecki/lina/internal/dbw"
)

func CreateTmpTables(ctx context.Context, dbw *dbw.DBW) error {
	tx, err := dbw.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("dbw.DB.BeginTx %w", err)
	}
	qtx := dbw.SQLC.WithTx(tx)

	err = qtx.CreateTmpDatasetDegreesTable(ctx)
	if err != nil {
		return fmt.Errorf("qtx.CreateTmpDatasetDegreesTable %w", err)
	}
	err = qtx.CreateTmpDatasetIndustriesTable(ctx)
	if err != nil {
		return fmt.Errorf("qtx.CreateTmpDatasetIndustriesTable %w", err)
	}
	err = qtx.CreateTmpDatasetLocationsTable(ctx)
	if err != nil {
		return fmt.Errorf("qtx.CreateTmpDatasetLocationsTable %w", err)
	}
	err = qtx.CreateTmpDatasetSkillsTable(ctx)
	if err != nil {
		return fmt.Errorf("qtx.CreateTmpDatasetSkillsTable %w", err)
	}
	err = qtx.CreateTmpDatasetSpecialtiesTable(ctx)
	if err != nil {
		return fmt.Errorf("qtx.CreateTmpDatasetSpecialtiesTable %w", err)
	}
	err = qtx.CreateTmpDatasetStudyFieldsTable(ctx)
	if err != nil {
		return fmt.Errorf("qtx.CreateTmpDatasetStudyFieldsTable %w", err)
	}
	err = qtx.CreateTmpOrganizationsTable(ctx)
	if err != nil {
		return fmt.Errorf("qtx.CreateTmpOrganizationsTable %w", err)
	}
	err = qtx.CreateTmpOrganizationLocationsTable(ctx)
	if err != nil {
		return fmt.Errorf("qtx.CreateTmpOrganizationLocationsTable %w", err)
	}
	err = qtx.CreateTmpOrganizationSpecialtiesTable(ctx)
	if err != nil {
		return fmt.Errorf("qtx.CreateTmpOrganizationSpecialtiesTable %w", err)
	}
	err = qtx.CreateTmpOrganizationIndustriesTable(ctx)
	if err != nil {
		return fmt.Errorf("qtx.CreateTmpOrganizationIndustriesTable %w", err)
	}
	err = qtx.CreateTmpPersonsTable(ctx)
	if err != nil {
		return fmt.Errorf("qtx.CreateTmpPersonsTable %w", err)
	}
	_, err = tx.Exec(`
		CREATE INDEX IF NOT EXISTS index_tmp_person_name ON tmp_persons(last_name, first_name);
		CREATE INDEX IF NOT EXISTS index_tmp_person_location ON tmp_persons(location);
	`)
	if err != nil {
		return fmt.Errorf("tx.Exec CREATE tmp_persons indexes %w", err)
	}
	err = qtx.CreateTmpPersonSkillsTable(ctx)
	if err != nil {
		return fmt.Errorf("qtx.CreateTmpPersonSkillsTable %w", err)
	}
	err = qtx.CreateTmpNetworkConnectionsTable(ctx)
	if err != nil {
		return fmt.Errorf("qtx.CreateTmpNetworkConnectionsTable %w", err)
	}
	err = qtx.CreateTmpExperiencesTable(ctx)
	if err != nil {
		return fmt.Errorf("qtx.CreateTmpExperiencesTable %w", err)
	}
	_, err = tx.Exec(`
		CREATE INDEX IF NOT EXISTS index_tmp_experience_person_organization ON tmp_experiences(person_id, organization_id);
	`)
	if err != nil {
		return fmt.Errorf("tx.Exec CREATE tmp_experiences indexes %w", err)
	}
	err = qtx.CreateTmpEducationsTable(ctx)
	if err != nil {
		return fmt.Errorf("qtx.CreateTmpEducationsTable %w", err)
	}
	_, err = tx.Exec(`
		CREATE INDEX IF NOT EXISTS index_tmp_education_person_organization ON tmp_educations(person_id, organization_id);
		CREATE INDEX IF NOT EXISTS index_tmp_education_degree ON tmp_educations(degree);
		CREATE INDEX IF NOT EXISTS index_tmp_education_study_field ON tmp_educations(study_field);
	`)
	if err != nil {
		return fmt.Errorf("tx.Exec CREATE tmp_educations indexes %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("tx.Commit %w", err)
	}

	return nil
}

func parseMonth(month string) *int64 {
	var m int64
	switch strings.ToLower(month) {
	case "jan":
		m = 1
	case "feb":
		m = 2
	case "mar":
		m = 3
	case "apr":
		m = 4
	case "may":
		m = 5
	case "jun":
		m = 6
	case "jul":
		m = 7
	case "aug":
		m = 8
	case "sep":
		m = 9
	case "oct":
		m = 10
	case "nov":
		m = 11
	case "dec":
		m = 12
	default:
		return nil
	}
	return &m
}
