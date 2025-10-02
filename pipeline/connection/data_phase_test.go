package connection

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/c-malecki/go-utils/gen"
	"github.com/c-malecki/go-utils/parse/pstring"
	"github.com/c-malecki/go-utils/path"
	"github.com/c-malecki/lina/internal/model"
	"github.com/c-malecki/lina/pipeline"
)

type TestDataPhase struct {
	startTime time.Time
	endTime   time.Time
	pipeline  *ConnectionPipeline
	next      pipeline.Phase
	test      *testing.T
}

func (p *TestDataPhase) Ended(err error) {
	p.endTime = time.Now()
	if err != nil {
		p.pipeline.End(pipeline.FAILED)
	}
}

func (p *TestDataPhase) Next() pipeline.Phase {
	return p.next
}

func (p *TestDataPhase) Start(ctx context.Context) {
	p.startTime = time.Now()
	p.pipeline.SetCurrent(p)
	fmt.Printf("\n=== data phase ===\n")

	fmt.Printf("\ntmp tables:\n\n")

	if ct, err := p.pipeline.dbw.SQLC.CountTmpConnections(ctx); err == nil {
		fmt.Printf("%d tmp connections\n", ct)
	}

	if ct, err := p.pipeline.dbw.SQLC.CountTmpPersons(ctx); err == nil {
		fmt.Printf("%d tmp persons\n", ct)
	}
	if ct, err := p.pipeline.dbw.SQLC.CountTmpPersonSkills(ctx); err == nil {
		fmt.Printf("%d tmp person skills\n", ct)
	}
	if ct, err := p.pipeline.dbw.SQLC.CountTmpExperiences(ctx); err == nil {
		fmt.Printf("%d tmp experiences\n", ct)
	}
	if ct, err := p.pipeline.dbw.SQLC.CountTmpEducations(ctx); err == nil {
		fmt.Printf("%d tmp educations\n\n", ct)
	}

	if ct, err := p.pipeline.dbw.SQLC.CountTmpOrganizations(ctx); err == nil {
		fmt.Printf("%d tmp organizations\n", ct)
	}
	if ct, err := p.pipeline.dbw.SQLC.CountTmpCompanies(ctx); err == nil {
		fmt.Printf("%d tmp companies\n", ct)
	}
	if ct, err := p.pipeline.dbw.SQLC.CountTmpSchools(ctx); err == nil {
		fmt.Printf("%d tmp schools\n", ct)
	}
	if ct, err := p.pipeline.dbw.SQLC.CountTmpOrganizationIndustries(ctx); err == nil {
		fmt.Printf("%d tmp org industries\n", ct)
	}
	if ct, err := p.pipeline.dbw.SQLC.CountTmpOrganizationSpecialties(ctx); err == nil {
		fmt.Printf("%d tmp org specialties\n", ct)
	}
	if ct, err := p.pipeline.dbw.SQLC.CountTmpOrganizationLocations(ctx); err == nil {
		fmt.Printf("%d tmp org locations\n\n", ct)
	}

	if ct, err := p.pipeline.dbw.SQLC.CountTmpDatasetDegrees(ctx); err == nil {
		fmt.Printf("%d tmp dataset degrees\n", ct)
	}
	if ct, err := p.pipeline.dbw.SQLC.CountTmpDatasetStudyFields(ctx); err == nil {
		fmt.Printf("%d tmp dataset study fields\n", ct)
	}
	if ct, err := p.pipeline.dbw.SQLC.CountTmpDatasetIndustries(ctx); err == nil {
		fmt.Printf("%d tmp dataset industries\n", ct)
	}
	if ct, err := p.pipeline.dbw.SQLC.CountTmpDatasetSkills(ctx); err == nil {
		fmt.Printf("%d tmp dataset skills\n", ct)
	}
	if ct, err := p.pipeline.dbw.SQLC.CountTmpDatasetSpecialies(ctx); err == nil {
		fmt.Printf("%d tmp dataset specialties\n", ct)
	}
	if ct, err := p.pipeline.dbw.SQLC.CountTmpDatasetLocations(ctx); err == nil {
		fmt.Printf("%d tmp dataset locations\n", ct)
	}

	if err := insertDatasetDataFromTmp(ctx, p.pipeline.dbw); err != nil {
		p.Ended(err)
		p.test.Fatal(err)
		return
	}

	if err := insertOrganizationDataFromTmp(ctx, p.pipeline.dbw); err != nil {
		p.Ended(err)
		p.test.Fatal(err)
		return
	}

	if err := insertPersonDataFromTmp(ctx, p.pipeline.dbw); err != nil {
		p.Ended(err)
		p.test.Fatal(err)
		return
	}

	fmt.Printf("\nreal tables:\n\n")

	if ct, err := p.pipeline.dbw.SQLC.CountPersons(ctx); err == nil {
		fmt.Printf("%d persons\n", ct)
	}
	if ct, err := p.pipeline.dbw.SQLC.CountPersonSkills(ctx); err == nil {
		fmt.Printf("%d person skills\n", ct)
	}
	if ct, err := p.pipeline.dbw.SQLC.CountExperiences(ctx); err == nil {
		fmt.Printf("%d experiences\n", ct)
	}
	if ct, err := p.pipeline.dbw.SQLC.CountEducations(ctx); err == nil {
		fmt.Printf("%d educations\n\n", ct)
	}

	if ct, err := p.pipeline.dbw.SQLC.CountOrganizations(ctx); err == nil {
		fmt.Printf("%d organizations\n", ct)
	}
	if ct, err := p.pipeline.dbw.SQLC.CountCompanies(ctx); err == nil {
		fmt.Printf("%d companies\n", ct)
	}
	if ct, err := p.pipeline.dbw.SQLC.CountSchools(ctx); err == nil {
		fmt.Printf("%d schools\n", ct)
	}
	if ct, err := p.pipeline.dbw.SQLC.CountOrganizationIndustries(ctx); err == nil {
		fmt.Printf("%d org industries\n", ct)
	}
	if ct, err := p.pipeline.dbw.SQLC.CountOrganizationSpecialties(ctx); err == nil {
		fmt.Printf("%d org specialties\n", ct)
	}
	if ct, err := p.pipeline.dbw.SQLC.CountOrganizationLocations(ctx); err == nil {
		fmt.Printf("%d org locations\n\n", ct)
	}

	if ct, err := p.pipeline.dbw.SQLC.CountDatasetDegrees(ctx); err == nil {
		fmt.Printf("%d dataset degrees\n", ct)
	}
	if ct, err := p.pipeline.dbw.SQLC.CountDatasetStudyFields(ctx); err == nil {
		fmt.Printf("%d dataset study fields\n", ct)
	}
	if ct, err := p.pipeline.dbw.SQLC.CountDatasetIndustries(ctx); err == nil {
		fmt.Printf("%d dataset industries\n", ct)
	}
	if ct, err := p.pipeline.dbw.SQLC.CountDatasetSkills(ctx); err == nil {
		fmt.Printf("%d dataset skills\n", ct)
	}
	if ct, err := p.pipeline.dbw.SQLC.CountDatasetSpecialties(ctx); err == nil {
		fmt.Printf("%d dataset specialties\n", ct)
	}
	if ct, err := p.pipeline.dbw.SQLC.CountDatasetLocations(ctx); err == nil {
		fmt.Printf("%d dataset locations\n\n", ct)
	}

	dir, err := path.FindProjectRoot()
	if err != nil {
		p.test.Fatalf("path.FindProjectRoot %v", err)
	}

	tmpExps, err := p.pipeline.dbw.SQLC.SelectTestTmpExperiences(ctx)
	if err != nil {
		p.test.Fatalf("SelectTestTmpExperiences %v", err)
	}

	if len(tmpExps) > 0 {
		if err := gen.GenOutputCsv(
			filepath.Join(dir, "dev", "output", "tmp_exp.csv"),
			[]string{"id", "person_urn", "organization_urn", "organization_name", "title", "location_raw", "start_year", "start_month", "is_current", "end_year", "end_month", "skills_url"},
			tmpExps,
			func(t model.SelectTestTmpExperiencesRow) []string {
				var row []string
				row = append(row,
					pstring.SafeNumPtrToStr(&t.ID),
					t.PersonUrn,
					t.OrganizationUrn,
					t.OrganizationName,
					t.Title,
					pstring.SafeStrPtr(t.LocationRaw),
					pstring.SafeNumPtrToStr(t.StartYear),
					pstring.SafeNumPtrToStr(t.StartMonth),
					pstring.SafeNumPtrToStr(&t.IsCurrent),
					pstring.SafeNumPtrToStr(t.EndYear),
					pstring.SafeNumPtrToStr(t.EndMonth),
					pstring.SafeStrPtr(t.SkillsUrl),
				)
				return row
			}); err != nil {
			p.test.Fatalf("createCsv tmp_exp.csv %v", err)
		}
	}

	exps, err := p.pipeline.dbw.SQLC.SelectTestExperiences(ctx)
	if err != nil {
		p.test.Fatalf("SelectTestExperiences %v", err)
	}

	if len(exps) > 0 {
		if err := gen.GenOutputCsv(
			filepath.Join(dir, "dev", "output", "exp.csv"),
			[]string{"id", "person_urn", "organization_urn", "organization_name", "title", "location_raw", "start_year", "start_month", "is_current", "end_year", "end_month", "skills_url"},
			exps,
			func(t model.SelectTestExperiencesRow) []string {
				var row []string
				row = append(row,
					pstring.SafeNumPtrToStr(&t.ID),
					t.PersonUrn,
					t.OrganizationUrn,
					t.OrganizationName,
					t.Title,
					pstring.SafeStrPtr(t.LocationRaw),
					pstring.SafeNumPtrToStr(t.StartYear),
					pstring.SafeNumPtrToStr(t.StartMonth),
					pstring.SafeNumPtrToStr(&t.IsCurrent),
					pstring.SafeNumPtrToStr(t.EndYear),
					pstring.SafeNumPtrToStr(t.EndMonth),
					pstring.SafeStrPtr(t.SkillsUrl),
				)
				return row
			}); err != nil {
			p.test.Fatalf("createCsv exp.csv %v", err)
		}
	}

	tmpEdus, err := p.pipeline.dbw.SQLC.SelectTestTmpEducations(ctx)
	if err != nil {
		p.test.Fatalf("SelectTestTmpEducations %v", err)
	}

	if len(tmpEdus) > 0 {
		if err := gen.GenOutputCsv(
			filepath.Join(dir, "dev", "output", "tmp_edu.csv"),
			[]string{"id", "person_urn", "organization_urn", "organization_name", "degree", "study_field", "start_year", "start_month", "end_year", "end_month"},
			tmpEdus,
			func(t model.SelectTestTmpEducationsRow) []string {
				var row []string
				row = append(row,
					pstring.SafeNumPtrToStr(&t.ID),
					t.PersonUrn,
					t.OrganizationUrn,
					t.OrganizationName,
					pstring.SafeStrPtr(t.Degree),
					pstring.SafeStrPtr(t.StudyField),
					pstring.SafeNumPtrToStr(t.StartYear),
					pstring.SafeNumPtrToStr(t.StartMonth),
					pstring.SafeNumPtrToStr(t.EndYear),
					pstring.SafeNumPtrToStr(t.EndMonth),
				)
				return row
			}); err != nil {
			p.test.Fatalf("createCsv tmp_edu.csv %v", err)
		}
	}

	edus, err := p.pipeline.dbw.SQLC.SelectTestEducations(ctx)
	if err != nil {
		p.test.Fatalf("SelectTestEducations %v", err)
	}

	if len(edus) > 0 {
		if err := gen.GenOutputCsv(
			filepath.Join(dir, "dev", "output", "edu.csv"),
			[]string{"id", "person_urn", "organization_urn", "organization_name", "degree", "study_field", "start_year", "start_month", "end_year", "end_month"},
			edus,
			func(t model.SelectTestEducationsRow) []string {
				var row []string
				row = append(row,
					pstring.SafeNumPtrToStr(&t.ID),
					t.PersonUrn,
					t.OrganizationUrn,
					t.OrganizationName,
					pstring.SafeStrPtr(t.Degree),
					pstring.SafeStrPtr(t.StudyField),
					pstring.SafeNumPtrToStr(t.StartYear),
					pstring.SafeNumPtrToStr(t.StartMonth),
					pstring.SafeNumPtrToStr(t.EndYear),
					pstring.SafeNumPtrToStr(t.EndMonth),
				)
				return row
			}); err != nil {
			p.test.Fatalf("createCsv edu.csv %v", err)
		}
	}

	if companies, _ := p.pipeline.dbw.SQLC.SelectTestCompanies(ctx); len(companies) > 0 {
		if err := gen.GenOutputCsv(
			filepath.Join(dir, "dev", "output", "companies.csv"),
			[]string{"id", "name", "universal", "website", "profile_url", "logo_url", "founded_year", "founded_month", "organization_type", "employee_count", "student_count", "urn"},
			companies,
			func(t model.Organizations) []string {
				var row []string
				row = append(row,
					pstring.SafeNumPtrToStr(&t.ID),
					t.Name,
					t.UniversalName,
					pstring.SafeStrPtr(t.Website),
					t.ProfileUrl,
					pstring.SafeStrPtr(t.LogoUrl),
					pstring.SafeNumPtrToStr(t.FoundedYear),
					pstring.SafeNumPtrToStr(t.FoundedMonth),
					pstring.SafeNumPtrToStr(&t.OrganizationType),
					pstring.SafeNumPtrToStr(t.EmployeeCount),
					pstring.SafeNumPtrToStr(t.StudentCount),
					t.Urn,
				)
				return row
			}); err != nil {
			p.test.Fatalf("createCsv edu.csv %v", err)
		}
	}

	if schools, _ := p.pipeline.dbw.SQLC.SelectTestSchools(ctx); len(schools) > 0 {
		if err := gen.GenOutputCsv(
			filepath.Join(dir, "dev", "output", "schools.csv"),
			[]string{"id", "name", "universal", "website", "profile_url", "logo_url", "founded_year", "founded_month", "organization_type", "employee_count", "student_count", "urn"},
			schools,
			func(t model.Organizations) []string {
				var row []string
				row = append(row,
					pstring.SafeNumPtrToStr(&t.ID),
					t.Name,
					t.UniversalName,
					pstring.SafeStrPtr(t.Website),
					t.ProfileUrl,
					pstring.SafeStrPtr(t.LogoUrl),
					pstring.SafeNumPtrToStr(t.FoundedYear),
					pstring.SafeNumPtrToStr(t.FoundedMonth),
					pstring.SafeNumPtrToStr(&t.OrganizationType),
					pstring.SafeNumPtrToStr(t.EmployeeCount),
					pstring.SafeNumPtrToStr(t.StudentCount),
					t.Urn,
				)
				return row
			}); err != nil {
			p.test.Fatalf("createCsv edu.csv %v", err)
		}
	}

	if persons, _ := p.pipeline.dbw.SQLC.SelectTestTmpPersons(ctx); len(persons) > 0 {
		if err := gen.GenOutputCsv(
			filepath.Join(dir, "dev", "output", "tmp_persons.csv"),
			[]string{"id", "first_name", "last_name", "headline", "profile_url", "public_identifier", "profile_picture_url", "location_id", "urn"},
			persons,
			func(t model.TmpPersons) []string {
				var row []string
				row = append(row,
					pstring.SafeNumPtrToStr(&t.ID),
					t.FirstName,
					t.LastName,
					pstring.SafeStrPtr(t.Headline),
					t.ProfileUrl,
					pstring.SafeStrPtr(&t.PublicIdentifier),
					pstring.SafeStrPtr(t.ProfilePictureUrl),
					pstring.SafeStrPtr(t.Location),
					t.Urn,
				)
				return row
			}); err != nil {
			p.test.Fatalf("createCsv edu.csv %v", err)
		}
	}

	if persons, _ := p.pipeline.dbw.SQLC.SelectTestPersons(ctx); len(persons) > 0 {
		if err := gen.GenOutputCsv(
			filepath.Join(dir, "dev", "output", "persons.csv"),
			[]string{"id", "first_name", "last_name", "headline", "profile_url", "public_identifier", "profile_picture_url", "location_id", "urn"},
			persons,
			func(t model.Persons) []string {
				var row []string
				row = append(row,
					pstring.SafeNumPtrToStr(&t.ID),
					t.FirstName,
					t.LastName,
					pstring.SafeStrPtr(t.Headline),
					t.ProfileUrl,
					pstring.SafeStrPtr(&t.PublicIdentifier),
					pstring.SafeStrPtr(t.ProfilePictureUrl),
					pstring.SafeNumPtrToStr(t.LocationID),
					t.Urn,
				)
				return row
			}); err != nil {
			p.test.Fatalf("createCsv edu.csv %v", err)
		}
	}

	p.next.Start(ctx)
}
