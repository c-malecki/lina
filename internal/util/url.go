package util

import (
	"fmt"
	"regexp"
)

func ExtractPersonLinkedin(url string) (string, error) {
	re := regexp.MustCompile(`(linkedin\.com\/in\/)([-a-zA-Z0-9@:%_+.~#&=]+)`)
	b := re.Find([]byte(url))
	if b == nil {
		return "", fmt.Errorf("\"%s\" is not a valid person linkedin url", url)
	}

	return "https://" + string(b), nil
}

func ExtractOrganizationLinkedin(url string) (string, error) {
	re := regexp.MustCompile(`(linkedin.com\/company\/|linkedin.com\/school\/)([-a-zA-Z0-9@:%_+.~#&=]+)`)
	b := re.Find([]byte(url))
	if b == nil {
		return "", fmt.Errorf("\"%s\" is not a valid organization linkedin url", url)
	}

	return "https://" + string(b), nil
}
