package network

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/c-malecki/lina/internal/apify"
	"github.com/c-malecki/lina/internal/dbw"
	"github.com/c-malecki/lina/internal/model"
)

type WrapPerson struct {
	CurrentCompanyURN *string
	model.InsertPersonParams
}

type WrapExp struct {
	OrganizationURN string
	Skills          []string
	model.InsertExperienceParams
}

type WrapEdu struct {
	OrganizationURN string
	Degree          *string
	StudyField      *string
	model.InsertEducationParams
}

type WrapOrganization struct {
	Specialties []string
	Industries  []string
	model.InsertOrganizationParams
}

type WrapOrgLocation struct {
	LocationName string
	model.InsertOrganizationLocationParams
}

type WrapOrgIndustry struct {
	Industry string
	model.InsertOrganizationIndustryParams
}

type WrapOrgSpecialty struct {
	Speciaialty string
	model.InsertOrganizationSpecialtyParams
}

func parseMonth(month string) *int64 {
	var m int64
	switch strings.ToLower(month) {
	case "jan":
		m = 1
	case "feb":
		m = 2
	case "mar":
		m = 3
	case "apr":
		m = 4
	case "may":
		m = 5
	case "jun":
		m = 6
	case "jul":
		m = 7
	case "aug":
		m = 8
	case "sep":
		m = 9
	case "oct":
		m = 10
	case "nov":
		m = 11
	case "dec":
		m = 12
	default:
		return nil
	}
	return &m
}

func RunApifyActors(ctx context.Context, DBW *dbw.DBW, token string, networkId int64, newUrls []string) error {
	if len(newUrls) == 0 {
		return nil
	}

	client := apify.NewApifyClient(token)

	personRun, err := client.RunActor(apify.LINKEDIN_PERSONS_ACTOR)
	if err != nil {
		return err
	}

	var personDatasetId string
	for {
		personRun, err = client.GetRun(personRun.Data.Id)
		if err != nil {
			return err
		}

		if personRun.Data.Status == string(apify.SUCCEEDED) {
			personDatasetId = personRun.Data.DefaultDatasetId
			break
		} else {
			time.Sleep(5 * time.Second)
		}
	}

	personDataset, err := client.GetDataset(personDatasetId)
	if err != nil {
		return err
	}

	var personResults []apify.Person
	if err := json.Unmarshal(personDataset, &personResults); err != nil {
		return err
	}

	if len(personResults) == 0 {
		return nil
	}

	seqsMap, err := DBW.QuerySeqs(ctx)
	if err != nil {
		return err
	}

	organizationUrls := make(map[string]string)

	locationsMap := make(map[string]model.InsertDatasetLocationParams)

	skillsMap := make(map[string]struct{})
	degreesMap := make(map[string]struct{})
	studyFieldsMap := make(map[string]struct{})

	var experiences []WrapExp
	var educations []WrapEdu

	createdAt := time.Now().Unix()

	var newPersons []WrapPerson
	for _, v := range personResults {
		personId := seqsMap["persons"]
		personId += 1
		seqsMap["persons"] = personId

		person := WrapPerson{
			InsertPersonParams: model.InsertPersonParams{
				ID:               personId,
				FirstName:        v.BasicInfo.FirstName,
				LastName:         v.BasicInfo.LastName,
				ProfileUrl:       v.BasicInfo.ProfileURL,
				PublicIdentifier: v.BasicInfo.PublicIdentifer,
				Urn:              v.BasicInfo.URN,
				CreatedAt:        createdAt,
			},
		}

		if len(v.BasicInfo.CurrentCompanyURN) > 0 {
			person.CurrentCompanyURN = &v.BasicInfo.CurrentCompanyURN
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

		if len(v.BasicInfo.Location.Full) > 0 {
			_, ok := locationsMap[v.BasicInfo.Location.Full]
			if !ok {
				id := seqsMap["locations"]
				id += 1
				seqsMap["locations"] = id

				location := model.InsertDatasetLocationParams{
					ID: id,
				}

				if len(v.BasicInfo.Location.City) > 0 {
					location.City = &v.BasicInfo.Location.City
				}

				if len(v.BasicInfo.Location.Country) > 0 {
					location.Country = &v.BasicInfo.Location.Country
				}

				if len(v.BasicInfo.Location.CountryCode) > 0 {
					location.CountryCode = &v.BasicInfo.Location.CountryCode
				}

				locationsMap[v.BasicInfo.Location.Full] = location
			}
		}

		for _, e := range v.Experience {
			_, ok := organizationUrls[e.CompanyID]
			if !ok {
				organizationUrls[e.CompanyID] = e.CompanyLinkedinURL
			}

			exp := WrapExp{
				OrganizationURN: e.CompanyID,
				Skills:          e.Skills,
				InsertExperienceParams: model.InsertExperienceParams{
					PersonID: personId,
					Title:    e.Title,
				},
			}

			for _, s := range e.Skills {
				if _, ok := skillsMap[s]; !ok {
					skillsMap[s] = struct{}{}
				}
			}

			if len(e.Location) > 0 {
				exp.LocationRaw = &e.Location
			}

			if len(e.Description) > 0 {
				exp.Description = &e.Description
			}

			if e.StartDate.Year != 0 {
				n := int64(e.StartDate.Year)
				exp.StartYear = &n
			}

			if e.StartDate.Month != nil {
				exp.StartMonth = parseMonth(*e.StartDate.Month)
			}

			if e.IsCurrent {
				exp.IsCurrent = 1
			}

			if e.EndDate.Year != 0 {
				n := int64(e.EndDate.Year)
				exp.StartYear = &n
			}

			if e.EndDate.Month != nil {
				exp.StartMonth = parseMonth(*e.EndDate.Month)
			}

			if len(e.SkillsURL) > 0 {
				exp.SkillsUrl = &e.SkillsURL
			}

			experiences = append(experiences, exp)
		}

		for _, e := range v.Education {
			_, ok := organizationUrls[e.SchoolID]
			if !ok {
				organizationUrls[e.SchoolID] = e.SchoolLinkedinURL
			}

			edu := WrapEdu{
				OrganizationURN: e.SchoolID,
				InsertEducationParams: model.InsertEducationParams{
					PersonID: personId,
				},
			}

			if len(e.Degree) > 0 {
				edu.Degree = &e.Degree
				if _, ok := degreesMap[e.Degree]; !ok {
					degreesMap[e.Degree] = struct{}{}
				}
			}

			if len(e.FieldOfStudy) > 0 {
				edu.StudyField = &e.FieldOfStudy
				if _, ok := studyFieldsMap[e.FieldOfStudy]; !ok {
					studyFieldsMap[e.FieldOfStudy] = struct{}{}
				}
			}

			if e.StartDate.Year != 0 {
				n := int64(e.StartDate.Year)
				edu.StartYear = &n
			}

			if e.StartDate.Month != nil {
				edu.StartMonth = parseMonth(*e.StartDate.Month)
			}

			if e.EndDate.Year != 0 {
				n := int64(e.EndDate.Year)
				edu.StartYear = &n
			}

			if e.EndDate.Month != nil {
				edu.StartMonth = parseMonth(*e.EndDate.Month)
			}

			educations = append(educations, edu)
		}
	}

	// insert new persons
	fmt.Printf("new persons: %d", len(newPersons))

	var checkOrganizationUrls []string
	for _, v := range organizationUrls {
		checkOrganizationUrls = append(checkOrganizationUrls, v)
	}

	existing, err := DBW.SQLC.SelectOrganizationsByLinkedinURLs(ctx, checkOrganizationUrls)
	if err != nil {
		return err
	}

	existingMap := make(map[string]struct{})
	for _, v := range existing {
		existingMap[v.ProfileUrl] = struct{}{}
	}

	var newOrganizationUrls []string
	for _, v := range checkOrganizationUrls {
		_, ok := existingMap[v]
		if !ok {
			newOrganizationUrls = append(newOrganizationUrls, v)
		}
	}

	var companyResults []apify.Company
	if len(newOrganizationUrls) > 0 {
		companyRun, err := client.RunActor(apify.LINKEDIN_COMPANIES_ACTOR)
		if err != nil {
			return err
		}

		var companyDatasetId string
		for {
			companyRun, err = client.GetRun(companyRun.Data.Id)
			if err != nil {
				return err
			}

			if companyRun.Data.Status == string(apify.SUCCEEDED) {
				companyDatasetId = companyRun.Data.DefaultDatasetId
				break
			} else {
				time.Sleep(5 * time.Second)
			}
		}

		companyDataset, err := client.GetDataset(companyDatasetId)
		if err != nil {
			return err
		}

		if err := json.Unmarshal(companyDataset, &companyResults); err != nil {
			return err
		}
	}

	// industriesMap := make(map[string]struct{})
	// specialtiesMap := make(map[string]struct{})

	// var orgLocations []WrapOrgLocation
	// var orgIndustries []WrapOrgIndustry
	// var WrapOrgSpecialty []WrapOrgSpecialty
	// var newOrganizations []WrapOrganization

	for _, v := range companyResults {
		organizationId := seqsMap["organizations"]
		organizationId += 1
		seqsMap["organizations"] = organizationId

		org := WrapOrganization{
			Industries:  v.BasicInfo.Industries,
			Specialties: v.BasicInfo.Specialties,
			InsertOrganizationParams: model.InsertOrganizationParams{
				ID:            organizationId,
				Name:          v.BasicInfo.Name,
				UniversalName: v.BasicInfo.UniversalName,
				ProfileUrl:    v.BasicInfo.LinkedinURL,
				Urn:           v.CompanyURN,
				CreatedAt:     createdAt,
			},
		}

		if len(v.BasicInfo.Website) > 0 {
			org.Website = &v.BasicInfo.Website
		}

		if len(v.Media.LogoURL) > 0 {
			org.LogoUrl = &v.Media.LogoURL
		}

		if v.BasicInfo.FoundedInfo.Year != 0 {
			n := int64(v.BasicInfo.FoundedInfo.Year)
			org.FoundedYear = &n
		}

		if v.BasicInfo.FoundedInfo.Month != nil {
			org.FoundedMonth = parseMonth(*v.BasicInfo.FoundedInfo.Month)
		}

		if v.BasicInfo.PageType == "COMPANY" {
			org.OrganizationType = 1
		} else {
			org.OrganizationType = 2
		}

		if v.Stats.EmployeeCount != 0 {
			n := int64(v.Stats.EmployeeCount)
			org.EmployeeCount = &n
		}

		if v.Stats.StudentCount != nil {
			n := int64(*v.Stats.StudentCount)
			org.StudentCount = &n
		}
	}

	return nil
}
