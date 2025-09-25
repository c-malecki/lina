package dbw

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	"github.com/c-malecki/lina/internal/model"
)

type DBW struct {
	DB   *sql.DB
	SQLC *model.Queries
}

func InitDB(ctx context.Context) (*DBW, error) {
	path, err := os.Executable()
	if err != nil {
		return nil, err
	}
	dir := filepath.Dir(path)

	db, err := sql.Open("sqlite", filepath.Join(dir, "data", "lina.db"))
	if err != nil {
		return nil, fmt.Errorf("sql.Open %w", err)
	}

	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("db.Ping %w", err)
	}

	dbw := &DBW{
		DB:   db,
		SQLC: model.New(db),
	}

	_, err = db.Exec(`PRAGMA foreign_keys = ON;`)
	if err != nil {
		return nil, fmt.Errorf("db.Exec \"PRAGMA foreign_keys = ON;\" %w", err)
	}

	tx, err := dbw.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("dbw.DB.BeginTx %w", err)
	}
	qtx := dbw.SQLC.WithTx(tx)

	err = qtx.CreateUsersTable(ctx)
	if err != nil {
		return nil, fmt.Errorf("qtx.CreateUsersTable %w", err)
	}
	err = qtx.CreateDatasetDegreesTable(ctx)
	if err != nil {
		return nil, fmt.Errorf("qtx.CreateDatasetDegreesTable %w", err)
	}
	err = qtx.CreateDatasetIndustriesTable(ctx)
	if err != nil {
		return nil, fmt.Errorf("qtx.CreateDatasetIndustriesTable %w", err)
	}
	err = qtx.CreateDatasetLocationsTable(ctx)
	if err != nil {
		return nil, fmt.Errorf("qtx.CreateDatasetLocationsTable %w", err)
	}
	err = qtx.CreateDatasetSkillsTable(ctx)
	if err != nil {
		return nil, fmt.Errorf("qtx.CreateDatasetSkillsTable %w", err)
	}
	err = qtx.CreateDatasetSpecialtiesTable(ctx)
	if err != nil {
		return nil, fmt.Errorf("qtx.CreateDatasetSpecialtiesTable %w", err)
	}
	err = qtx.CreateDatasetStudyFieldsTable(ctx)
	if err != nil {
		return nil, fmt.Errorf("qtx.CreateDatasetStudyFieldsTable %w", err)
	}
	err = qtx.CreateOrganizationsTable(ctx)
	if err != nil {
		return nil, fmt.Errorf("qtx.CreateOrganizationsTable %w", err)
	}
	_, err = tx.Exec(`CREATE INDEX IF NOT EXISTS index_organization_name ON organizations("name", universal_name);`)
	if err != nil {
		return nil, fmt.Errorf("tx.Exec CREATE organizations indexes %w", err)
	}
	err = qtx.CreateOrganizationLocationsTable(ctx)
	if err != nil {
		return nil, fmt.Errorf("qtx.CreateOrganizationLocationsTable %w", err)
	}
	err = qtx.CreateOrganizationSpecialtiesTable(ctx)
	if err != nil {
		return nil, fmt.Errorf("qtx.CreateOrganizationSpecialtiesTable %w", err)
	}
	err = qtx.CreateOrganizationIndustriesTable(ctx)
	if err != nil {
		return nil, fmt.Errorf("qtx.CreateOrganizationIndustriesTable %w", err)
	}
	err = qtx.CreatePersonsTable(ctx)
	if err != nil {
		return nil, fmt.Errorf("qtx.CreatePersonsTable %w", err)
	}
	_, err = tx.Exec(`
		CREATE INDEX IF NOT EXISTS index_person_name ON persons(last_name, first_name);
		CREATE INDEX IF NOT EXISTS index_person_location ON persons(location_id);
		CREATE INDEX IF NOT EXISTS index_person_current_company ON persons(current_company_id);
	`)
	if err != nil {
		return nil, fmt.Errorf("tx.Exec CREATE persons indexes %w", err)
	}
	err = qtx.CreateNetworksTable(ctx)
	if err != nil {
		return nil, fmt.Errorf("qtx.CreateNetworksTable %w", err)
	}
	err = qtx.CreateNetworkConnectionsTable(ctx)
	if err != nil {
		return nil, fmt.Errorf("qtx.CreateNetworkConnectionsTable %w", err)
	}
	err = qtx.CreateExperiencesTable(ctx)
	if err != nil {
		return nil, fmt.Errorf("qtx.CreateExperiencesTable %w", err)
	}
	_, err = tx.Exec(`
		CREATE INDEX IF NOT EXISTS index_experience_person_organization ON experiences(person_id, organization_id);
		CREATE INDEX IF NOT EXISTS index_experience_title ON experiences(title);
		CREATE INDEX IF NOT EXISTS index_experience_is_current ON experiences(is_current);
	`)
	if err != nil {
		return nil, fmt.Errorf("tx.Exec CREATE experiences indexes %w", err)
	}
	err = qtx.CreateEducationsTable(ctx)
	if err != nil {
		return nil, fmt.Errorf("qtx.CreateEducationsTable %w", err)
	}
	_, err = tx.Exec(`
		CREATE INDEX IF NOT EXISTS index_education_person_organization ON educations(person_id, organization_id);
		CREATE INDEX IF NOT EXISTS index_education_degree ON educations(degree_id);
		CREATE INDEX IF NOT EXISTS index_education_study_field ON educations(study_field_id);
	`)
	if err != nil {
		return nil, fmt.Errorf("tx.Exec CREATE educations indexes %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return nil, fmt.Errorf("tx.Commit %w", err)
	}

	return dbw, nil
}
