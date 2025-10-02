package main

import (
	"bufio"
	"context"
	"log"
	"os"
	"strings"

	"github.com/c-malecki/lina/internal/app"
	"github.com/c-malecki/lina/internal/dbw"
)

// todo: error handling with logging and friendly user message

func main() {
	DBW, err := dbw.NewDBW()
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	if err := dbw.InitSchema(ctx, DBW); err != nil {
		log.Fatal(err)
	}

	APP := &app.App{
		DBW: DBW,
	}

	err = APP.GetOrCreateUser(ctx)
	if err != nil {
		log.Fatal(err)
	}

	err = APP.PrintNetworkStats(ctx, DBW)
	if err != nil {
		log.Fatal(err)
	}

	for {
		APP.PrintActions()

		reader := bufio.NewReader(os.Stdin)
		act, _ := reader.ReadString('\n')
		act = strings.TrimSpace(act)

		err = APP.DispatchAction(ctx, DBW, act)
		if err != nil {
			log.Fatal(err)
		}
	}
}
