package connection

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/c-malecki/go-utils/path"
	"github.com/c-malecki/lina/internal/apify"
	"github.com/c-malecki/lina/internal/model"
	"github.com/c-malecki/lina/pipeline"
)

type TestRequestPhase struct {
	startTime time.Time
	endTime   time.Time
	pipeline  *ConnectionPipeline
	next      pipeline.Phase
	test      *testing.T
	testCount int
}

func (p *TestRequestPhase) Ended(err error) {
	p.endTime = time.Now()
	if err != nil {
		p.pipeline.End(pipeline.FAILED)
	}
}

func (p *TestRequestPhase) Next() pipeline.Phase {
	return p.next
}

func (p *TestRequestPhase) Start(ctx context.Context) {
	p.startTime = time.Now()
	p.pipeline.SetCurrent(p)
	fmt.Printf("\n=== request phase ===\n")

	newPersonUrls, err := p.pipeline.dbw.SQLC.SelectTmpConnectionsNoPersonIDs(ctx)
	if err != nil {
		p.Ended(err)
		p.test.Fatal(err)
		return
	}

	fmt.Printf("\n%d no person tmp connections\n", len(newPersonUrls))

	if len(newPersonUrls) == 0 {
		// skip to network phase
		p.Ended(nil)
		p.next.Next().Start(ctx)
		return
	}

	dir, err := path.FindProjectRoot()
	if err != nil {
		p.test.Fatalf("path.FindProjectRoot %v", err)
	}

	personData, err := os.ReadFile(filepath.Join(dir, "dev", "test", "person.json"))
	if err != nil {
		p.test.Fatalf("os.ReadFile %v", err)
	}

	var personResults []apify.Person
	if err := json.Unmarshal(personData, &personResults); err != nil {
		p.test.Fatalf("json.Unmarshal %v", err)
	}

	switch p.testCount {
	case 1:
		personResults = slices.DeleteFunc(personResults, func(t apify.Person) bool {
			return t.BasicInfo.URN == "ACoAACyOi_8BJQwcmkj8PUr3ajslq1Z-pha6Bh0"
		})
	case 2:
		personResults = slices.DeleteFunc(personResults, func(t apify.Person) bool {
			return t.BasicInfo.URN == "ACoAAABvcIMBE62G8XVDwrojVwjosCofW7Y2J7A"
		})
	}

	fmt.Printf("\n%d apify persons\n", len(personResults))

	if len(personResults) == 0 {
		// skip to network phase
		p.Ended(nil)
		p.next.Next().Start(ctx)
		return
	}

	extracted := &extracted{
		personSkills:   make(map[string][]string),
		orgIndustries:  make(map[string][]string),
		orgSpecialties: make(map[string][]string),
		orgLocations:   make(map[string][]model.InsertTmpOrganizationLocationParams),
	}
	aggregated := &aggregated{
		organizationUrls: make(map[string]string),
		locations:        make(map[string]model.InsertTmpDatasetLocationParams),
		skills:           make(map[string]struct{}),
		degrees:          make(map[string]struct{}),
		studyFields:      make(map[string]struct{}),
		specialties:      make(map[string]struct{}),
		industries:       make(map[string]struct{}),
	}

	for _, v := range personResults {
		extractPerson(v, extracted, aggregated, p.startTime)
		for _, e := range v.Experience {
			extractExperience(e, v.BasicInfo.URN, extracted, aggregated)
		}
		for _, e := range v.Education {
			extractEducation(e, v.BasicInfo.URN, extracted, aggregated)
		}
	}

	_, err = filterNewOrganizationURLs(ctx, p.pipeline.dbw, aggregated)
	if err != nil {
		p.Ended(err)
		return
	}

	organizationData, err := os.ReadFile(filepath.Join(dir, "dev", "test", "organization.json"))
	if err != nil {
		p.test.Fatalf("os.ReadFile %v", err)
	}

	var organizationResults []apify.Company
	if err := json.Unmarshal(organizationData, &organizationResults); err != nil {
		p.test.Fatalf("json.Unmarshal %v", err)
	}

	if p.testCount == 2 {
		organizationResults = slices.DeleteFunc(organizationResults, func(t apify.Company) bool {
			return t.CompanyURN == "75582279" || t.CompanyURN == "3290938" || t.CompanyURN == "104434622" || t.CompanyURN == "68065373"
		})
	}

	fmt.Printf("%d apify organizations\n", len(organizationResults))

	for _, v := range organizationResults {
		extractOrganization(v, extracted, aggregated, p.startTime)
	}

	if err := insertTmpPersonData(ctx, p.pipeline.dbw, extracted); err != nil {
		p.Ended(err)
		p.test.Fatalf("insertTmpPersonData %v", err)
		return
	}

	if err := insertTmpOrganizationData(ctx, p.pipeline.dbw, extracted); err != nil {
		p.Ended(err)
		p.test.Fatalf("insertTmpOrganizationData %v", err)
		return
	}

	if err := insertTmpDatasetData(ctx, p.pipeline.dbw, aggregated); err != nil {
		p.Ended(err)
		p.test.Fatalf("insertTmpDatasetData %v", err)
		return
	}

	p.next.Start(ctx)
}
