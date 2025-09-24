package action

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/c-malecki/lina/internal/util"
)

const bom = "\uFEFF"

var liCsvHeaders = [...]string{"first name", "last name", "url", "email address", "company", "position", "connected on"}

func ValidateLinkedinCsv(file *os.File) ([]string, error) {
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
	for {
		line, err := reader.Read()

		if err == io.EOF {
			break
		} else if err != nil {
			return nil, fmt.Errorf("read csv %w", err)
		}

		li, err := util.ExtractPersonLinkedin(line[0])
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
	fmt.Printf("Invalid entries: %d\n", invalidCt)

	return linkedins, nil
}
