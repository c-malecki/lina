package apify

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/c-malecki/lina/internal/dbw"
	"github.com/c-malecki/lina/internal/model"
)

func TestProcessPersonResponses(t *testing.T) {
	ctx := context.Background()

	dir, err := dbw.FindRootDir()
	if err != nil {
		t.Fatalf("dbw.FindRootDir %v", err)
	}

	DBW, err := dbw.InitTestDBW(dir)
	if err != nil {
		t.Fatalf("dbw.InitDB %v", err)
	}

	// get new persons from apify
	pb, err := os.ReadFile(filepath.Join(dir, "testdata", "person.json"))
	if err != nil {
		t.Fatalf("os.ReadFile %v", err)
	}

	seqsMap, err := DBW.QuerySeqs(ctx)
	if err != nil {
		t.Fatalf("DBW.QuerySeqs %v", err)
	}

	var newPersons []Person
	if err := json.Unmarshal(pb, &newPersons); err != nil {
		t.Fatalf("json.Unmarshal( %v", err)
	}

	var persons []model.InsertPersonParams
	organizations := make(map[string]string)

	createdAt := time.Now().Unix()

	for _, v := range newPersons {
		id := seqsMap["persons"]
		id += 1
		person := model.InsertPersonParams{
			ID:               id,
			FirstName:        v.BasicInfo.FirstName,
			LastName:         v.BasicInfo.LastName,
			ProfileUrl:       v.BasicInfo.ProfileURL,
			PublicIdentifier: v.BasicInfo.PublicIdentifer,
			Urn:              v.BasicInfo.URN,
			// CurrentCompanyID: id if org exists or after inserting new org,
			CreatedAt: createdAt,
		}

		if len(v.BasicInfo.Headline) > 0 {
			person.Headline = &v.BasicInfo.Headline
		}

		if len(v.BasicInfo.ProfilePictureURL) > 0 {
			person.ProfilePictureUrl = &v.BasicInfo.ProfilePictureURL
		}

		if len(v.BasicInfo.About) > 0 {
			person.About = &v.BasicInfo.About
		}

		for _, exp := range v.Experience {
			_, ok := organizations[exp.CompanyID]
			if !ok {
				organizations[exp.CompanyID] = exp.CompanyLinkedinURL
			}
		}

		for _, edu := range v.Education {
			_, ok := organizations[edu.SchoolID]
			if !ok {
				organizations[edu.SchoolID] = edu.SchoolLinkedinURL
			}
		}
	}

	// check for organizations
	// get new organizations from apify

	ob, err := os.ReadFile(filepath.Join(dir, "testdata", "company.json"))
	if err != nil {
		t.Fatalf("os.ReadFile %v", err)
	}

	var testOrganizations []Company
	if err := json.Unmarshal(ob, &testOrganizations); err != nil {
		t.Fatalf("json.Unmarshal( %v", err)
	}

	// locationsMap := make(map[string]struct{})

	// skillsMap := make(map[string]struct{})
	// degreesMap := make(map[string]struct{})
	// studyFieldsMap := make(map[string]struct{})

	// industriesMap := make(map[string]struct{})
	// specialtiesMap := make(map[string]struct{})

	// var experiences []Experience
	// var educations []Education
}
