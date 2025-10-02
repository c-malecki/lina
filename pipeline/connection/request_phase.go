package connection

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/c-malecki/go-utils/database"
	"github.com/c-malecki/lina/internal/apify"
	"github.com/c-malecki/lina/internal/dbw"
	"github.com/c-malecki/lina/internal/model"
	"github.com/c-malecki/lina/pipeline"
)

type extracted struct {
	persons        []model.InsertTmpPersonParams
	personSkills   map[string][]string
	experiences    []model.InsertTmpExperienceParams
	educations     []model.InsertTmpEducationParams
	organizations  []model.InsertTmpOrganizationParams
	orgLocations   map[string][]model.InsertTmpOrganizationLocationParams
	orgIndustries  map[string][]string
	orgSpecialties map[string][]string
}

type aggregated struct {
	organizationUrls map[string]string
	locations        map[string]model.InsertTmpDatasetLocationParams
	skills           map[string]struct{}
	degrees          map[string]struct{}
	studyFields      map[string]struct{}
	specialties      map[string]struct{}
	industries       map[string]struct{}
}

type RequestPhase struct {
	startTime time.Time
	endTime   time.Time
	pipeline  *ConnectionPipeline
	next      pipeline.Phase
}

func (p *RequestPhase) Ended(err error) {
	p.endTime = time.Now()
	if err != nil {
		p.pipeline.End(pipeline.FAILED)
	}
}

func (p *RequestPhase) Next() pipeline.Phase {
	return p.next
}

func (p *RequestPhase) Start(ctx context.Context) {
	p.startTime = time.Now()
	p.pipeline.SetCurrent(p)

	newPersonUrls, err := p.pipeline.dbw.SQLC.SelectTmpConnectionsNoPersonIDs(ctx)
	if err != nil {
		p.Ended(err)
		return
	}

	if len(newPersonUrls) == 0 {
		// skip to network phase
		p.Ended(nil)
		p.next.Next().Start(ctx)
		return
	}

	client := apify.NewApifyClient(*p.pipeline.user.ApifyToken)

	personResults, err := apify.RunActorAndGetResults(client, apify.LINKEDIN_PERSONS_ACTOR, newPersonUrls, []apify.Person{})
	if err != nil {
		p.Ended(err)
		return
	}

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

	newOrgUrls, err := filterNewOrganizationURLs(ctx, p.pipeline.dbw, aggregated)
	if err != nil {
		p.Ended(err)
		return
	}

	organizationResults, err := apify.RunActorAndGetResults(client, apify.LINKEDIN_COMPANIES_ACTOR, newOrgUrls, []apify.Company{})
	if err != nil {
		p.Ended(err)
		return
	}

	for _, v := range organizationResults {
		extractOrganization(v, extracted, aggregated, p.startTime)
	}

	if err := insertTmpPersonData(ctx, p.pipeline.dbw, extracted); err != nil {
		p.Ended(err)
		return
	}

	if err := insertTmpOrganizationData(ctx, p.pipeline.dbw, extracted); err != nil {
		p.Ended(err)
		return
	}

	if err := insertTmpDatasetData(ctx, p.pipeline.dbw, aggregated); err != nil {
		p.Ended(err)
		return
	}

	p.next.Start(ctx)
}

func extractPerson(data apify.Person, ex *extracted, ag *aggregated, startTime time.Time) {
	person := model.InsertTmpPersonParams{
		FirstName:        data.BasicInfo.FirstName,
		LastName:         data.BasicInfo.LastName,
		ProfileUrl:       data.BasicInfo.ProfileURL,
		PublicIdentifier: data.BasicInfo.PublicIdentifer,
		Urn:              data.BasicInfo.URN,
		CreatedAt:        startTime.Unix(),
	}

	if len(data.BasicInfo.Headline) > 0 {
		person.Headline = &data.BasicInfo.Headline
	}

	if len(data.BasicInfo.ProfilePictureURL) > 0 {
		person.ProfilePictureUrl = &data.BasicInfo.ProfilePictureURL
	}

	if len(data.BasicInfo.About) > 0 {
		person.About = &data.BasicInfo.About
	}

	if len(data.BasicInfo.Location.City) > 0 || len(data.BasicInfo.Location.Country) > 0 {
		name := strings.Join([]string{data.BasicInfo.Location.City, data.BasicInfo.Location.Country}, ", ")
		if len(name) > 0 {
			_, ok := ag.locations[name]
			if !ok {
				location := model.InsertTmpDatasetLocationParams{
					Name: name,
				}

				if len(data.BasicInfo.Location.City) > 0 {
					location.City = &data.BasicInfo.Location.City
				}

				if len(data.BasicInfo.Location.Country) > 0 {
					location.Country = &data.BasicInfo.Location.Country
				}

				ag.locations[name] = location
			}
		}
	}

	ex.persons = append(ex.persons, person)
}

func extractExperience(data apify.Experience, personUrn string, ex *extracted, ag *aggregated) {
	_, ok := ag.organizationUrls[data.CompanyID]
	if !ok {
		ag.organizationUrls[data.CompanyID] = data.CompanyLinkedinURL
	}

	exp := model.InsertTmpExperienceParams{
		PersonUrn:       personUrn,
		OrganizationUrn: data.CompanyID,
		Title:           data.Title,
	}

	for _, s := range data.Skills {
		if _, ok := ag.skills[s]; !ok {
			ag.skills[s] = struct{}{}
		}
		if m, ok := ex.personSkills[s]; !ok {
			ex.personSkills[s] = []string{personUrn}
		} else {
			if !slices.Contains(m, personUrn) {
				m = append(m, personUrn)
				ex.personSkills[s] = m
			}
		}
	}

	if len(data.Location) > 0 {
		exp.LocationRaw = &data.Location
	}

	if len(data.Description) > 0 {
		exp.Description = &data.Description
	}

	if data.StartDate.Year != 0 {
		n := int64(data.StartDate.Year)
		exp.StartYear = &n
	}

	if data.StartDate.Month != nil {
		exp.StartMonth = parseMonth(*data.StartDate.Month)
	}

	if data.IsCurrent {
		exp.IsCurrent = 1
	}

	if data.EndDate.Year != 0 {
		n := int64(data.EndDate.Year)
		exp.StartYear = &n
	}

	if data.EndDate.Month != nil {
		exp.StartMonth = parseMonth(*data.EndDate.Month)
	}

	if len(data.SkillsURL) > 0 {
		exp.SkillsUrl = &data.SkillsURL
	}

	ex.experiences = append(ex.experiences, exp)
}

func extractEducation(data apify.Education, personUrn string, ex *extracted, ag *aggregated) {
	_, ok := ag.organizationUrls[data.SchoolID]
	if !ok {
		ag.organizationUrls[data.SchoolID] = data.SchoolLinkedinURL
	}

	edu := model.InsertTmpEducationParams{
		PersonUrn:       personUrn,
		OrganizationUrn: data.SchoolID,
	}

	if len(data.Degree) > 0 {
		edu.Degree = &data.Degree
		if _, ok := ag.degrees[data.Degree]; !ok {
			ag.degrees[data.Degree] = struct{}{}
		}
	}

	if len(data.FieldOfStudy) > 0 {
		edu.StudyField = &data.FieldOfStudy
		if _, ok := ag.studyFields[data.FieldOfStudy]; !ok {
			ag.studyFields[data.FieldOfStudy] = struct{}{}
		}
	}

	if data.StartDate.Year != 0 {
		n := int64(data.StartDate.Year)
		edu.StartYear = &n
	}

	if data.StartDate.Month != nil {
		edu.StartMonth = parseMonth(*data.StartDate.Month)
	}

	if data.EndDate.Year != 0 {
		n := int64(data.EndDate.Year)
		edu.StartYear = &n
	}

	if data.EndDate.Month != nil {
		edu.StartMonth = parseMonth(*data.EndDate.Month)
	}

	ex.educations = append(ex.educations, edu)
}

func filterNewOrganizationURLs(ctx context.Context, dbw *dbw.DBW, ag *aggregated) ([]string, error) {
	var profileUrls []string
	for _, v := range ag.organizationUrls {
		profileUrls = append(profileUrls, v)
	}

	existing, err := dbw.SQLC.SelectOrganizationsByLinkedinURLs(ctx, profileUrls)
	if err != nil {
		return nil, err
	}

	existingMap := make(map[string]struct{})
	for _, v := range existing {
		existingMap[v.ProfileUrl] = struct{}{}
	}

	var newProfileUrls []string
	for _, v := range profileUrls {
		_, ok := existingMap[v]
		if !ok {
			newProfileUrls = append(newProfileUrls, v)
		}
	}

	return newProfileUrls, nil
}

func extractOrganization(data apify.Company, ex *extracted, ag *aggregated, startTime time.Time) {
	org := model.InsertTmpOrganizationParams{
		Name:          data.BasicInfo.Name,
		UniversalName: data.BasicInfo.UniversalName,
		ProfileUrl:    data.BasicInfo.LinkedinURL,
		Urn:           data.CompanyURN,
		CreatedAt:     startTime.Unix(),
	}

	if len(data.BasicInfo.Website) > 0 {
		org.Website = &data.BasicInfo.Website
	}

	if len(data.Media.LogoURL) > 0 {
		org.LogoUrl = &data.Media.LogoURL
	}

	if data.BasicInfo.FoundedInfo.Year != 0 {
		n := int64(data.BasicInfo.FoundedInfo.Year)
		org.FoundedYear = &n
	}

	if data.BasicInfo.FoundedInfo.Month != nil {
		org.FoundedMonth = parseMonth(*data.BasicInfo.FoundedInfo.Month)
	}

	if data.BasicInfo.PageType == "COMPANY" {
		org.OrganizationType = 1
	} else {
		org.OrganizationType = 2
	}

	if data.Stats.EmployeeCount != 0 {
		n := int64(data.Stats.EmployeeCount)
		org.EmployeeCount = &n
	}

	if data.Stats.StudentCount != nil {
		n := int64(*data.Stats.StudentCount)
		org.StudentCount = &n
	}

	for _, s := range data.BasicInfo.Specialties {
		if _, ok := ag.specialties[s]; !ok {
			ag.specialties[s] = struct{}{}
		}
		if m, ok := ex.orgSpecialties[s]; !ok {
			ex.orgSpecialties[s] = []string{data.CompanyURN}
		} else {
			if !slices.Contains(m, data.CompanyURN) {
				m = append(m, data.CompanyURN)
				ex.orgSpecialties[s] = m
			}
		}
	}

	for _, s := range data.BasicInfo.Industries {
		if _, ok := ag.industries[s]; !ok {
			ag.industries[s] = struct{}{}
		}
		if m, ok := ex.orgIndustries[s]; !ok {
			ex.orgIndustries[s] = []string{data.CompanyURN}
		} else {
			if !slices.Contains(m, data.CompanyURN) {
				m = append(m, data.CompanyURN)
				ex.orgIndustries[s] = m
			}
		}
	}

	if len(data.Locations.Headquarters.City) > 0 || len(data.Locations.Headquarters.State) > 0 || len(data.Locations.Headquarters.Country) > 0 {
		name := strings.Join([]string{data.Locations.Headquarters.City, data.Locations.Headquarters.State, data.Locations.Headquarters.Country}, ", ")
		if len(name) > 0 {
			_, ok := ag.locations[name]
			if !ok {
				location := model.InsertTmpDatasetLocationParams{
					Name: name,
				}

				if len(data.Locations.Headquarters.City) > 0 {
					location.City = &data.Locations.Headquarters.City
				}

				if len(data.Locations.Headquarters.State) > 0 {
					location.State = &data.Locations.Headquarters.State
				}

				if len(data.Locations.Headquarters.Country) > 0 {
					location.Country = &data.Locations.Headquarters.Country
				}

				ag.locations[name] = location
			}

			if m, ok := ex.orgLocations[data.CompanyURN]; !ok {
				ex.orgLocations[data.CompanyURN] = []model.InsertTmpOrganizationLocationParams{{
					OrganizationUrn: data.CompanyURN,
					Location:        name,
					IsHeadquarters:  1,
				}}
			} else {
				if !slices.ContainsFunc(m, func(l model.InsertTmpOrganizationLocationParams) bool {
					return l.Location == name && l.OrganizationUrn == data.CompanyURN
				}) {
					m = append(m, model.InsertTmpOrganizationLocationParams{
						OrganizationUrn: data.CompanyURN,
						Location:        name,
						IsHeadquarters:  1,
					})
					ex.orgLocations[data.CompanyURN] = m
				}
			}
		}
	}

	for _, l := range data.Locations.Offices {
		name := strings.Join([]string{l.City, l.State, l.Country}, ", ")
		if len(name) == 0 {
			continue
		}

		_, ok := ag.locations[name]
		if !ok {
			location := model.InsertTmpDatasetLocationParams{
				Name: name,
			}

			if len(l.City) > 0 {
				location.City = &l.City
			}

			if len(l.State) > 0 {
				location.State = &l.State
			}

			if len(l.Country) > 0 {
				location.Country = &l.Country
			}

			ag.locations[name] = location
		}

		if m, ok := ex.orgLocations[data.CompanyURN]; !ok {
			ex.orgLocations[data.CompanyURN] = []model.InsertTmpOrganizationLocationParams{{
				OrganizationUrn: data.CompanyURN,
				Location:        name,
				IsHeadquarters:  0,
			}}
		} else {
			if !slices.ContainsFunc(m, func(l model.InsertTmpOrganizationLocationParams) bool {
				return l.Location == name && l.OrganizationUrn == data.CompanyURN
			}) {
				m = append(m, model.InsertTmpOrganizationLocationParams{
					OrganizationUrn: data.CompanyURN,
					Location:        name,
					IsHeadquarters:  0,
				})
				ex.orgLocations[data.CompanyURN] = m
			}
		}
	}

	ex.organizations = append(ex.organizations, org)
}

func insertTmpPersonData(ctx context.Context, dbw *dbw.DBW, ex *extracted) error {
	err := dbw.SQLC.CreateTmpPersonsTable(ctx)
	if err != nil {
		return fmt.Errorf("dbw.SQLC.CreateTmpPersonsTable %w", err)
	}

	_, err = dbw.DB.Exec(`
		CREATE INDEX IF NOT EXISTS index_tmp_person_name ON tmp_persons(last_name, first_name);
		CREATE INDEX IF NOT EXISTS index_tmp_person_location ON tmp_persons(location);
	`)
	if err != nil {
		return fmt.Errorf("dbw.DB.Exec CREATE tmp_persons indexes %w", err)
	}

	err = dbw.SQLC.CreateTmpPersonSkillsTable(ctx)
	if err != nil {
		return fmt.Errorf("dbw.SQLC.CreateTmpPersonSkillsTable %w", err)
	}

	err = dbw.SQLC.CreateTmpExperiencesTable(ctx)
	if err != nil {
		return fmt.Errorf("dbw.SQLC.CreateTmpExperiencesTable %w", err)
	}
	_, err = dbw.DB.Exec(`
		CREATE INDEX IF NOT EXISTS index_tmp_experience_person_organization ON tmp_experiences(person_urn, organization_urn);
	`)
	if err != nil {
		return fmt.Errorf("dbw.DB.Exec CREATE tmp_experiences indexes %w", err)
	}

	err = dbw.SQLC.CreateTmpEducationsTable(ctx)
	if err != nil {
		return fmt.Errorf("dbw.SQLC.CreateTmpEducationsTable %w", err)
	}
	_, err = dbw.DB.Exec(`
		CREATE INDEX IF NOT EXISTS index_tmp_education_person_organization ON tmp_educations(person_urn, organization_urn);
		CREATE INDEX IF NOT EXISTS index_tmp_education_degree ON tmp_educations(degree);
		CREATE INDEX IF NOT EXISTS index_tmp_education_study_field ON tmp_educations(study_field);
	`)

	if err != nil {
		return fmt.Errorf("tx.Exec CREATE tmp_educations indexes %w", err)
	}

	if _, err := database.BatchInsert(ctx, dbw.DB, database.BatchInsertDesc[model.InsertTmpPersonParams]{
		Query: model.InsertTmpPerson,
		Items: ex.persons,
		ExtractFn: func(itpp model.InsertTmpPersonParams) []interface{} {
			return []interface{}{
				itpp.FirstName,
				itpp.LastName,
				itpp.Headline,
				itpp.ProfileUrl,
				itpp.PublicIdentifier,
				itpp.ProfilePictureUrl,
				itpp.About,
				itpp.Location,
				itpp.Urn,
				itpp.CreatedAt,
			}
		},
	}); err != nil {
		return err
	}

	var personSkills []model.InsertTmpPersonSkillParams
	for k, v := range ex.personSkills {
		for _, urn := range v {
			personSkills = append(personSkills, model.InsertTmpPersonSkillParams{
				PersonUrn: urn,
				Skill:     k,
			})
		}
	}

	if _, err := database.BatchInsert(ctx, dbw.DB, database.BatchInsertDesc[model.InsertTmpPersonSkillParams]{
		Query: model.InsertTmpPersonSkill,
		Items: personSkills,
		ExtractFn: func(itpsp model.InsertTmpPersonSkillParams) []interface{} {
			return []interface{}{
				itpsp.PersonUrn,
				itpsp.Skill,
			}
		},
	}); err != nil {
		return err
	}

	if _, err := database.BatchInsert(ctx, dbw.DB, database.BatchInsertDesc[model.InsertTmpExperienceParams]{
		Query: model.InsertTmpExperience,
		Items: ex.experiences,
		ExtractFn: func(itep model.InsertTmpExperienceParams) []interface{} {
			return []interface{}{
				itep.PersonUrn,
				itep.OrganizationUrn,
				itep.Title,
				itep.LocationRaw,
				itep.Description,
				itep.StartYear,
				itep.StartMonth,
				itep.IsCurrent,
				itep.EndYear,
				itep.EndMonth,
				itep.SkillsUrl,
			}
		},
	}); err != nil {
		return err
	}

	if _, err := database.BatchInsert(ctx, dbw.DB, database.BatchInsertDesc[model.InsertTmpEducationParams]{
		Query: model.InsertTmpEducation,
		Items: ex.educations,
		ExtractFn: func(itep model.InsertTmpEducationParams) []interface{} {
			return []interface{}{
				itep.PersonUrn,
				itep.OrganizationUrn,
				itep.Degree,
				itep.StudyField,
				itep.StartYear,
				itep.StartMonth,
				itep.EndYear,
				itep.EndMonth,
			}
		},
	}); err != nil {
		return err
	}

	ex.persons = nil
	ex.personSkills = nil
	ex.experiences = nil
	ex.educations = nil

	return nil
}

func insertTmpOrganizationData(ctx context.Context, dbw *dbw.DBW, ex *extracted) error {
	err := dbw.SQLC.CreateTmpOrganizationsTable(ctx)
	if err != nil {
		return fmt.Errorf("dbw.SQLC.CreateTmpOrganizationsTable %w", err)
	}
	err = dbw.SQLC.CreateTmpOrganizationLocationsTable(ctx)
	if err != nil {
		return fmt.Errorf("dbw.SQLC.CreateTmpOrganizationLocationsTable %w", err)
	}
	err = dbw.SQLC.CreateTmpOrganizationSpecialtiesTable(ctx)
	if err != nil {
		return fmt.Errorf("dbw.SQLC.CreateTmpOrganizationSpecialtiesTable %w", err)
	}
	err = dbw.SQLC.CreateTmpOrganizationIndustriesTable(ctx)
	if err != nil {
		return fmt.Errorf("dbw.SQLC.CreateTmpOrganizationIndustriesTable %w", err)
	}

	if _, err := database.BatchInsert(ctx, dbw.DB, database.BatchInsertDesc[model.InsertTmpOrganizationParams]{
		Query: model.InsertTmpOrganization,
		Items: ex.organizations,
		ExtractFn: func(itop model.InsertTmpOrganizationParams) []interface{} {
			return []interface{}{
				itop.Name,
				itop.UniversalName,
				itop.Website,
				itop.ProfileUrl,
				itop.LogoUrl,
				itop.FoundedYear,
				itop.FoundedMonth,
				itop.OrganizationType,
				itop.EmployeeCount,
				itop.StudentCount,
				itop.Urn,
				itop.CreatedAt,
			}
		},
	}); err != nil {
		return err
	}

	var orgIndustries []model.InsertTmpOrganizationIndustryParams
	for k, v := range ex.orgIndustries {
		for _, urn := range v {
			orgIndustries = append(orgIndustries, model.InsertTmpOrganizationIndustryParams{
				OrganizationUrn: urn,
				Industry:        k,
			})
		}
	}

	if _, err := database.BatchInsert(ctx, dbw.DB, database.BatchInsertDesc[model.InsertTmpOrganizationIndustryParams]{
		Query: model.InsertTmpOrganizationIndustry,
		Items: orgIndustries,
		ExtractFn: func(itop model.InsertTmpOrganizationIndustryParams) []interface{} {
			return []interface{}{
				itop.OrganizationUrn,
				itop.Industry,
			}
		},
	}); err != nil {
		return err
	}

	var orgSpecialties []model.InsertTmpOrganizationSpecialtyParams
	for k, v := range ex.orgSpecialties {
		for _, urn := range v {
			orgSpecialties = append(orgSpecialties, model.InsertTmpOrganizationSpecialtyParams{
				OrganizationUrn: urn,
				Specialty:       k,
			})
		}
	}

	if _, err := database.BatchInsert(ctx, dbw.DB, database.BatchInsertDesc[model.InsertTmpOrganizationSpecialtyParams]{
		Query: model.InsertTmpOrganizationSpecialty,
		Items: orgSpecialties,
		ExtractFn: func(itop model.InsertTmpOrganizationSpecialtyParams) []interface{} {
			return []interface{}{
				itop.OrganizationUrn,
				itop.Specialty,
			}
		},
	}); err != nil {
		return err
	}

	var orgLocations []model.InsertTmpOrganizationLocationParams
	for _, params := range ex.orgLocations {
		orgLocations = append(orgLocations, params...)
	}

	if _, err := database.BatchInsert(ctx, dbw.DB, database.BatchInsertDesc[model.InsertTmpOrganizationLocationParams]{
		Query: model.InsertTmpOrganizationLocation,
		Items: orgLocations,
		ExtractFn: func(itop model.InsertTmpOrganizationLocationParams) []interface{} {
			return []interface{}{
				itop.OrganizationUrn,
				itop.Location,
				itop.IsHeadquarters,
			}
		},
	}); err != nil {
		return err
	}

	return nil
}

func insertTmpDatasetData(ctx context.Context, dbw *dbw.DBW, ag *aggregated) error {
	err := dbw.SQLC.CreateTmpDatasetDegreesTable(ctx)
	if err != nil {
		return fmt.Errorf("dbw.SQLC.CreateTmpDatasetDegreesTable %w", err)
	}
	err = dbw.SQLC.CreateTmpDatasetIndustriesTable(ctx)
	if err != nil {
		return fmt.Errorf("dbw.SQLC.CreateTmpDatasetIndustriesTable %w", err)
	}
	err = dbw.SQLC.CreateTmpDatasetLocationsTable(ctx)
	if err != nil {
		return fmt.Errorf("dbw.SQLC.CreateTmpDatasetLocationsTable %w", err)
	}
	err = dbw.SQLC.CreateTmpDatasetSkillsTable(ctx)
	if err != nil {
		return fmt.Errorf("dbw.SQLC.CreateTmpDatasetSkillsTable %w", err)
	}
	err = dbw.SQLC.CreateTmpDatasetSpecialtiesTable(ctx)
	if err != nil {
		return fmt.Errorf("dbw.SQLC.CreateTmpDatasetSpecialtiesTable %w", err)
	}
	err = dbw.SQLC.CreateTmpDatasetStudyFieldsTable(ctx)
	if err != nil {
		return fmt.Errorf("dbw.SQLC.CreateTmpDatasetStudyFieldsTable %w", err)
	}

	var degrees []string
	for k := range ag.degrees {
		degrees = append(degrees, k)
	}

	if _, err := database.BatchInsert(ctx, dbw.DB, database.BatchInsertDesc[string]{
		Query: model.InsertTmpDatasetDegree,
		Items: degrees,
		ExtractFn: func(s string) []interface{} {
			return []interface{}{
				s,
			}
		},
	}); err != nil {
		return err
	}

	var industries []string
	for k := range ag.industries {
		industries = append(industries, k)
	}

	if _, err := database.BatchInsert(ctx, dbw.DB, database.BatchInsertDesc[string]{
		Query: model.InsertTmpDatasetIndustry,
		Items: industries,
		ExtractFn: func(s string) []interface{} {
			return []interface{}{
				s,
			}
		},
	}); err != nil {
		return err
	}

	var locations []model.InsertTmpDatasetLocationParams
	for _, l := range ag.locations {
		locations = append(locations, l)
	}

	if _, err := database.BatchInsert(ctx, dbw.DB, database.BatchInsertDesc[model.InsertTmpDatasetLocationParams]{
		Query: model.InsertTmpDatasetLocation,
		Items: locations,
		ExtractFn: func(itdlp model.InsertTmpDatasetLocationParams) []interface{} {
			return []interface{}{
				itdlp.Name,
				itdlp.City,
				itdlp.State,
				itdlp.Country,
			}
		},
	}); err != nil {
		return err
	}

	var skills []string
	for k := range ag.skills {
		skills = append(skills, k)
	}

	if _, err := database.BatchInsert(ctx, dbw.DB, database.BatchInsertDesc[string]{
		Query: model.InsertTmpDatasetSkill,
		Items: skills,
		ExtractFn: func(s string) []interface{} {
			return []interface{}{
				s,
			}
		},
	}); err != nil {
		return err
	}

	var specialties []string
	for k := range ag.specialties {
		specialties = append(specialties, k)
	}

	if _, err := database.BatchInsert(ctx, dbw.DB, database.BatchInsertDesc[string]{
		Query: model.InsertTmpDatasetSpecialty,
		Items: specialties,
		ExtractFn: func(s string) []interface{} {
			return []interface{}{
				s,
			}
		},
	}); err != nil {
		return err
	}

	var studyFields []string
	for k := range ag.studyFields {
		studyFields = append(studyFields, k)
	}

	if _, err := database.BatchInsert(ctx, dbw.DB, database.BatchInsertDesc[string]{
		Query: model.InsertTmpDatasetStudyField,
		Items: studyFields,
		ExtractFn: func(s string) []interface{} {
			return []interface{}{
				s,
			}
		},
	}); err != nil {
		return err
	}

	return nil
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
