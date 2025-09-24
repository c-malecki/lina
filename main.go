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
	_ "modernc.org/sqlite"
)

func main() {
	ctx := context.Background()
	DBW, err := dbw.InitDB(ctx)
	if err != nil {
		log.Fatal(err)
	}

	token, err := action.GetOrSetConfig(ctx, DBW)
	if err != nil {
		log.Fatal(err)
	}
	println(token)

	for {
		err := action.ShowStats(ctx, DBW)
		if err != nil {
			log.Fatal(err)
		}
		action.ShowMenu()

		reader := bufio.NewReader(os.Stdin)
		choice, _ := reader.ReadString('\n')
		choice = strings.TrimSpace(choice)

		switch choice {
		case "1":
			fmt.Println("Updating connections...")
		case "2":
			fmt.Println("Search is currently disabled")
		case "3":
			return
		default:
			fmt.Println("Invalid option")
		}
		fmt.Println()
	}
}
