package network

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/c-malecki/lina/internal/dbw"
)

func CompareConnections(ctx context.Context, DBW *dbw.DBW, networkId int64, urls []string) ([]string, error) {
	persons, err := DBW.SQLC.SelectPersonsByLinkedinURLs(ctx, urls)
	if err != nil {
		return nil, err
	}

	personMap := make(map[string]int64)
	for _, v := range persons {
		personMap[v.ProfileUrl] = v.ID
	}

	var newPersonUrls []string
	existingMap := make(map[string]int64)

	for _, v := range urls {
		p, ok := personMap[v]
		if !ok {
			newPersonUrls = append(newPersonUrls, v)
		} else {
			existingMap[v] = p
		}
	}

	connectionsMap := make(map[string]int64)
	connections, err := DBW.SQLC.SelectPersonsByNetworkConnections(ctx, networkId)
	if err != nil {
		return nil, err
	}

	var removeConnections []int64
	for _, v := range connections {
		connectionsMap[v.ProfileUrl] = v.ID
		_, ok := existingMap[v.ProfileUrl]
		if !ok {
			removeConnections = append(removeConnections, v.ID)
		}
	}

	var addConnections []int64
	for k, v := range existingMap {
		_, ok := connectionsMap[k]
		if !ok {
			addConnections = append(addConnections, v)
		}
	}

	fmt.Printf("\n\n%d new connections will be added and %d connections will be removed\n", len(addConnections)+len(newPersonUrls), len(removeConnections))
	fmt.Print("Do you wish to proceed? [Y/n] ")

	reader := bufio.NewReader(os.Stdin)
	opt, _ := reader.ReadString('\n')
	opt = strings.TrimSpace(opt)

	if opt == "n" {
		return nil, nil
	}

	return newPersonUrls, nil
}
