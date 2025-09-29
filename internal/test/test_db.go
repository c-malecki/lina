package test

import (
	"database/sql"
	"fmt"
	"path/filepath"

	"github.com/c-malecki/lina/internal/dbw"
	"github.com/c-malecki/lina/internal/model"
)

func InitTestDBW(dir string) (*dbw.DBW, error) {
	db, err := sql.Open("sqlite", filepath.Join(dir, "build", "data", "lina.db"))
	if err != nil {
		return nil, fmt.Errorf("sql.Open %w", err)
	}

	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("db.Ping %w", err)
	}

	dbw := &dbw.DBW{
		DB:   db,
		SQLC: model.New(db),
	}

	return dbw, nil
}
