package action

import (
	"bufio"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/c-malecki/lina/internal/dbw"
	"github.com/c-malecki/lina/internal/util"
)

func ParseLinkedinCsv() ([]string, error) {
	var valid bool
	fmt.Print("\nPath to LinkedIn connections.csv: ")

	var liUrls []string
	for !valid {
		reader := bufio.NewReader(os.Stdin)
		path, _ := reader.ReadString('\n')
		path = strings.TrimSpace(path)

		csv, err := os.Open(path)
		if err != nil {
			return nil, err
		}

		urls, err := validateLinkedinCsv(csv)
		if err != nil {
			return nil, err
		}

		liUrls = urls
		valid = true
	}

	return liUrls, nil
}

func ProceedWithEnrichment(ctx context.Context, DBW *dbw.DBW, state *util.State, urls []string) error {
	persons, err := DBW.SQLC.SelectPersonsByLinkedinURLs(ctx, urls)
	if err != nil {
		return err
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
	connections, err := DBW.SQLC.SelectPersonsByNetworkConnections(ctx, state.Network.ID)
	if err != nil {
		return err
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
		return nil
	}

	// do apify stuff

	return nil
}

const bom = "\uFEFF"

var liCsvHeaders = [...]string{"first name", "last name", "url", "email address", "company", "position", "connected on"}

func validateLinkedinCsv(file *os.File) ([]string, error) {
	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1

	var headers []string

	for {
		line, err := reader.Read()

		if err == io.EOF {
			return nil, errors.New("csv does not match the expected format")
		} else if err != nil {
			return nil, fmt.Errorf("error: read csv line %w", err)
		}

		firstCol := line[0]

		if hasBom := strings.HasPrefix(firstCol, bom); hasBom {
			firstCol = strings.TrimPrefix(firstCol, bom)
			line[0] = firstCol
		}

		if isSame := strings.EqualFold(firstCol, "first name"); isSame {
			headers = line
			break
		}
	}

	if len(headers) != len(liCsvHeaders) {
		return nil, fmt.Errorf("csv headers length (%v) does not match expected length (%v)", len(headers), len(liCsvHeaders))
	}

	for i, h := range headers {
		isSame := strings.EqualFold(h, liCsvHeaders[i])
		if !isSame {
			return nil, fmt.Errorf("parsed header \"%s\" does not match expected header \"%s\"", h, liCsvHeaders[i])
		}
	}

	liMap := make(map[string]struct{})

	validCt := 0
	invalidCt := 0

	fmt.Println("\nParsing CSV...")

	for {
		line, err := reader.Read()

		if err == io.EOF {
			break
		} else if err != nil {
			return nil, fmt.Errorf("read csv %w", err)
		}

		li, err := util.ExtractPersonLinkedin(line[2])
		if err != nil {
			invalidCt += 1
			continue
		}

		if _, ok := liMap[li]; !ok {
			liMap[li] = struct{}{}
		}
		validCt += 1
	}

	var linkedins []string
	for k := range liMap {
		linkedins = append(linkedins, k)
	}

	fmt.Printf("Valid entries: %d\n", validCt)
	fmt.Printf("Invalid entries: %d", invalidCt)

	return linkedins, nil
}
