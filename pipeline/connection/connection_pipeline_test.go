package connection

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/c-malecki/go-utils/path"
	"github.com/c-malecki/lina/dev/test"
	"github.com/c-malecki/lina/internal/dbw"
	"github.com/c-malecki/lina/pipeline"
)

func Test_ConnectionPipeline(t *testing.T) {
	dir, err := path.FindProjectRoot()
	if err != nil {
		t.Fatalf("path.FindProjectRoot %v", err)
	}

	t.Cleanup(func() {
		os.Remove(filepath.Join(dir, "build", "data", "test.db"))
	})

	DBW, err := dbw.NewTestDBW(dir)
	if err != nil {
		t.Fatalf("dbw.NewTestDBW %v", err)
	}

	ctx := context.Background()
	if err := dbw.InitSchema(ctx, DBW); err != nil {
		t.Fatalf("dbw.InitSchema %v", err)
	}

	if _, err := DBW.DB.ExecContext(ctx, test.SetupTestDBW); err != nil {
		t.Fatalf("dbw.InitSchema %v", err)
	}

	users, err := DBW.SQLC.SelectUsers(ctx)
	if err != nil || len(users) == 0 {
		t.Fatalf("DBW.SQLC.SelectUsers %v", err)
	}

	network, err := DBW.SQLC.SelectNetworkByUserID(ctx, users[0].ID)
	if err != nil {
		t.Fatalf("DBW.SQLC.SelectNetworkByUserID %v", err)
	}

	t1, err := os.Open(filepath.Join(dir, "dev", "test", "test1.csv"))
	if err != nil {
		t.Fatalf("os.Open %v", err)
	}

	p := &ConnectionPipeline{
		user:      &users[0],
		network:   &network,
		dbw:       DBW,
		startTime: time.Now(),
		status:    pipeline.RUNNING,
	}

	net := &TestNetworkPhase{pipeline: p, test: t}
	data := &TestDataPhase{pipeline: p, next: net, test: t}
	request := &TestRequestPhase{pipeline: p, next: data, test: t, testCount: 1}
	compare := &TestComparePhase{pipeline: p, next: request, test: t}
	csv := &TestCsvPhase{pipeline: p, next: compare, csv: t1, test: t}

	csv.Start(context.Background())
	t.Logf("\n%d test completed\n", request.testCount)
	request.testCount += 1

	if _, err := DBW.DB.ExecContext(ctx, `
		DROP TABLE IF EXISTS tmp_connections;
		DROP TABLE IF EXISTS tmp_persons;
		DROP TABLE IF EXISTS tmp_person_skills;
		DROP TABLE IF EXISTS tmp_experiences;
		DROP TABLE IF EXISTS tmp_educations;
		DROP TABLE IF EXISTS tmp_dataset_degrees;
		DROP TABLE IF EXISTS tmp_dataset_study_fields;
		DROP TABLE IF EXISTS tmp_dataset_skills;
		DROP TABLE IF EXISTS tmp_dataset_locations;
		DROP TABLE IF EXISTS tmp_dataset_industries;
		DROP TABLE IF EXISTS tmp_dataset_specialties;
		DROP TABLE IF EXISTS tmp_organizations;
		DROP TABLE IF EXISTS tmp_organization_industries;
		DROP TABLE IF EXISTS tmp_organization_specialties;
		DROP TABLE IF EXISTS tmp_organization_locations;
	`); err != nil {
		t.Fatalf("drop tmp tables %v", err)
	}

	t2, err := os.Open(filepath.Join(dir, "dev", "test", "test2.csv"))
	if err != nil {
		t.Fatalf("os.Open %v", err)
	}
	csv.csv = t2

	csv.Start(context.Background())
	t.Logf("\n%d test completed\n", request.testCount)
	request.testCount += 1

	if _, err := DBW.DB.ExecContext(ctx, `
		DROP TABLE IF EXISTS tmp_connections;
		DROP TABLE IF EXISTS tmp_persons;
		DROP TABLE IF EXISTS tmp_person_skills;
		DROP TABLE IF EXISTS tmp_experiences;
		DROP TABLE IF EXISTS tmp_educations;
		DROP TABLE IF EXISTS tmp_dataset_degrees;
		DROP TABLE IF EXISTS tmp_dataset_study_fields;
		DROP TABLE IF EXISTS tmp_dataset_skills;
		DROP TABLE IF EXISTS tmp_dataset_locations;
		DROP TABLE IF EXISTS tmp_dataset_industries;
		DROP TABLE IF EXISTS tmp_dataset_specialties;
		DROP TABLE IF EXISTS tmp_organizations;
		DROP TABLE IF EXISTS tmp_organization_industries;
		DROP TABLE IF EXISTS tmp_organization_specialties;
		DROP TABLE IF EXISTS tmp_organization_locations;
	`); err != nil {
		t.Fatalf("drop tmp tables %v", err)
	}

	t3, err := os.Open(filepath.Join(dir, "dev", "test", "test3.csv"))
	if err != nil {
		t.Fatalf("os.Open %v", err)
	}
	csv.csv = t3

	csv.Start(context.Background())

	t.Logf("\n%d test completed\n", request.testCount)
}
