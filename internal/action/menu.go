package action

import (
	"fmt"
)

func ShowMenu() {
	fmt.Print("\nOptions:\n\n")
	fmt.Println("1. Update Connections")
	fmt.Println("2. Search (disabled)")
	fmt.Println("3. Quit")
	fmt.Print("\nSelection: ")
}
