package action

import (
	"fmt"
)

func ShowOptions() {
	fmt.Print("\nOptions:\n\n")
	fmt.Println("1. Update Connections")
	fmt.Println("2. Search (disabled)")
	fmt.Println("3. Set Apify token")
	fmt.Println("4. Quit")
	fmt.Print("\nSelection: ")
}
