package action

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/c-malecki/lina/internal/dbw"
	"github.com/c-malecki/lina/internal/model"
)

func GetOrSetConfig(ctx context.Context, DBW *dbw.DBW) (string, error) {
	cfg, err := DBW.SQLC.SelectConfig(ctx)
	if err != nil {
		return "", fmt.Errorf("DBW.SQLC.SelectConfig %w", err)
	}
	if len(cfg) == 0 {
		return "", fmt.Errorf("DBW.SQLC.SelectConfig no config found")
	}

	var token string

	if cfg[0].Secret == nil {
		fmt.Print("Enter Apify API secret: ")
		reader := bufio.NewReader(os.Stdin)
		token, _ = reader.ReadString('\n')
		token = strings.TrimSpace(token)

		if token == "" {
			fmt.Println("API secret cannot be empty")
			return "", err
		}

		err = DBW.SQLC.UpdateConfigSecret(ctx, model.UpdateConfigSecretParams{
			ID:     cfg[0].ID,
			Secret: &token,
		})
		if err != nil {
			return "", err
		}

		fmt.Println("API secret saved successfully!")
	}

	return token, nil
}
