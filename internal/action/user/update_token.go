package user

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/c-malecki/lina/internal/dbw"
	"github.com/c-malecki/lina/internal/model"
)

func UpdateApifyToken(ctx context.Context, DBW *dbw.DBW, userId int64) (*string, error) {
	var token *string
	for token == nil {
		fmt.Print("\nUpdate Apify API token: ")

		reader := bufio.NewReader(os.Stdin)
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)

		if input == "" {
			fmt.Println("Apify API token cannot be empty")
			continue
		}

		// TODO: validate token format?

		err := DBW.SQLC.UpdateUserApifyToken(ctx, model.UpdateUserApifyTokenParams{
			ID:         userId,
			ApifyToken: &input,
		})
		if err != nil {
			return nil, err
		}

		fmt.Println("Apify API token saved")
		token = &input
	}

	return token, nil
}
