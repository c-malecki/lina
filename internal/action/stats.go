package action

import (
	"context"
	"fmt"

	"github.com/c-malecki/lina/internal/dbw"
)

func ShowStats(ctx context.Context, DBW *dbw.DBW) error {
	pct, err := DBW.SQLC.CountPersons(ctx)
	if err != nil {
		return err
	}
	cct, err := DBW.SQLC.CountCompanies(ctx)
	if err != nil {
		return err
	}
	sct, err := DBW.SQLC.CountSchools(ctx)
	if err != nil {
		return err
	}

	fmt.Printf("People Records: %d\n", pct)
	fmt.Printf("Company Records: %d\n", cct)
	fmt.Printf("School Records: %d\n", sct)

	return nil
}
