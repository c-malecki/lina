package app

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/c-malecki/lina/internal/action"
	"github.com/c-malecki/lina/internal/action/user"
	"github.com/c-malecki/lina/internal/dbw"
	"github.com/c-malecki/lina/internal/model"
	"github.com/c-malecki/lina/pipeline/connection"
	"golang.org/x/crypto/bcrypt"
)

type App struct {
	User          *model.Users
	Network       *model.Networks
	CurrentAction action.APP_ACTION
	DBW           *dbw.DBW
}

func (app *App) PrintActions() {
	fmt.Print("\nActions:\n\n")
	fmt.Println("1. Update Connections")
	fmt.Println("2. Search (disabled)")
	fmt.Println("3. Update Apify token")
	fmt.Println("4. Quit")
	fmt.Print("\nSelection: ")
}

func (app *App) PrintNetworkStats(ctx context.Context, DBW *dbw.DBW) error {
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

	fmt.Printf("\n%s:\n", app.Network.Name)
	fmt.Printf("\nPeople Records: %d\n", pct)
	fmt.Printf("Company Records: %d\n", cct)
	fmt.Printf("School Records: %d\n", sct)

	return nil
}

func (app *App) DispatchAction(ctx context.Context, dbw *dbw.DBW, act string) error {
	switch action.APP_ACTION(act) {
	case action.UPDATE_CONNECTIONS:
		app.CurrentAction = action.UPDATE_CONNECTIONS
		if app.User.ApifyToken == nil {
			token, err := user.UpdateApifyToken(ctx, dbw, app.User.ID)
			if err != nil {
				return err
			}
			app.User.ApifyToken = token
		}
		connection.InitConnectionPipeline(app.User, app.Network, app.DBW)
	case action.SEARCH:
		fmt.Println("Search is currently disabled")
		return nil
	case action.UPDATE_APIFY:
		token, err := user.UpdateApifyToken(ctx, dbw, app.User.ID)
		if err != nil {
			return err
		}
		app.User.ApifyToken = token
		return nil
	case action.QUIT:
		os.Exit(0)
	}
	fmt.Println()
	return nil
}

func (app *App) GetOrCreateUser(ctx context.Context, DBW *dbw.DBW) error {
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
				Username:  username,
				Password:  string(hash),
				CreatedAt: time.Now().Unix(),
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

			app.User = &user
			app.Network = &network
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

			app.User = &user
			app.Network = &network
		}
		authed = true
	}

	return nil
}
