package action

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"

	"github.com/c-malecki/lina/internal/dbw"
	"github.com/c-malecki/lina/internal/model"
	"github.com/c-malecki/lina/internal/util"
	"golang.org/x/crypto/bcrypt"
)

func GetOrCreateUser(ctx context.Context, DBW *dbw.DBW, state *util.State) error {
	ct, err := DBW.SQLC.CountUsers(ctx)
	if err != nil {
		return fmt.Errorf("CountUsers %w", err)
	}

	var authed bool

	for !authed {
		if ct == 0 {
			fmt.Println("Create new user")
		}
		fmt.Print("username: ")

		reader := bufio.NewReader(os.Stdin)
		username, _ := reader.ReadString('\n')
		username = strings.TrimSpace(username)

		if username == "" {
			fmt.Println("username cannot be empty")
			continue
		}

		fmt.Print("password: ")
		reader = bufio.NewReader(os.Stdin)
		password, _ := reader.ReadString('\n')
		password = strings.TrimSpace(password)

		if password == "" {
			fmt.Println("password cannot be empty")
			continue
		}

		if len(password) < 8 {
			fmt.Println("password must be at least 8 characters")
			continue
		}

		if ct == 0 {
			hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
			if err != nil {
				return err
			}

			err = DBW.SQLC.InsertUser(ctx, model.InsertUserParams{
				Username: username,
				Password: string(hash),
			})
			if err != nil {
				return err
			}

			user, err := DBW.SQLC.SelectUser(ctx, username)
			if err != nil {
				if err == sql.ErrNoRows {
					fmt.Println("user does not exist")
					continue
				} else {
					return err
				}
			}

			err = DBW.SQLC.InsertNetwork(ctx, model.InsertNetworkParams{
				UserID: user.ID,
				Name:   username + "'s network",
			})
			if err != nil {
				return err
			}

			network, err := DBW.SQLC.SelectNetworkByUserID(ctx, user.ID)
			if err != nil {
				return err
			}

			state.User = &user
			state.Network = &network
		} else {
			user, err := DBW.SQLC.SelectUser(ctx, username)
			if err != nil {
				if err == sql.ErrNoRows {
					fmt.Println("user does not exist")
					continue
				} else {
					return err
				}
			}

			err = bcrypt.CompareHashAndPassword([]byte(user.Password), []byte(password))
			if err != nil {
				fmt.Println("password is incorrect")
				continue
			}

			network, err := DBW.SQLC.SelectNetworkByUserID(ctx, user.ID)
			if err != nil {
				return err
			}

			state.User = &user
			state.Network = &network
		}
		authed = true
	}

	return nil
}

func UpdateApifyToken(ctx context.Context, DBW *dbw.DBW, state *util.State) error {
	for state.User.ApifyToken == nil {
		fmt.Print("\nSet Apify API token: ")

		reader := bufio.NewReader(os.Stdin)
		token, _ := reader.ReadString('\n')
		token = strings.TrimSpace(token)

		if token == "" {
			fmt.Println("Apify API token cannot be empty")
			continue
		}

		// validate token format?

		err := DBW.SQLC.UpdateUserApifyToken(ctx, model.UpdateUserApifyTokenParams{
			ID:         state.User.ID,
			ApifyToken: &token,
		})
		if err != nil {
			return err
		}

		fmt.Println("Apify API token saved")
		state.User.ApifyToken = &token
	}

	return nil
}
