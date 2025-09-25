package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/c-malecki/lina/internal/action"
	"github.com/c-malecki/lina/internal/dbw"
	"github.com/c-malecki/lina/internal/util"
	_ "modernc.org/sqlite"
)

func main() {
	ctx := context.Background()
	DBW, err := dbw.InitDB(ctx)
	if err != nil {
		log.Fatal(err)
	}

	state := &util.State{}

	err = action.GetOrCreateUser(ctx, DBW, state)
	if err != nil {
		log.Fatal(err)
	}

	for {
		err := action.ShowStats(ctx, DBW, state)
		if err != nil {
			log.Fatal(err)
		}

		action.ShowOptions()

		reader := bufio.NewReader(os.Stdin)
		opt, _ := reader.ReadString('\n')
		opt = strings.TrimSpace(opt)

		switch opt {
		case "1":
			if state.User.ApifyToken == nil {
				err = action.UpdateApifyToken(ctx, DBW, state)
				if err != nil {
					log.Fatal(err)
				}
			}
			urls, err := action.ParseLinkedinCsv()
			if err != nil {
				log.Fatal(err)
			}
			err = action.ProceedWithEnrichment(ctx, DBW, state, urls)
			if err != nil {
				log.Fatal(err)
			}
		case "2":
			fmt.Println("Search is currently disabled")
		case "3":

		case "4":
			return
		default:
			fmt.Println("Invalid option")
		}
		fmt.Println()
	}
}
