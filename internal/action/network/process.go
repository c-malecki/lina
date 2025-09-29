package network

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/c-malecki/go-utils/database"
	"github.com/c-malecki/lina/internal/apify"
	"github.com/c-malecki/lina/internal/dbw"
	"github.com/c-malecki/lina/internal/model"
)

type extract struct {
	Persons        []model.InsertTmpPersonParams
	PersonSkills   []model.InsertTmpPersonSkillParams
	Experiences    []model.InsertTmpExperienceParams
	Educations     []model.InsertTmpEducationParams
	Organizations  []model.InsertTmpOrganizationParams
	OrgLocations   []model.InsertTmpOrganizationLocationParams
	OrgIndustries  []model.InsertTmpOrganizationIndustryParams
	OrgSpecialties []model.InsertTmpOrganizationSpecialtyParams
}

type aggregate struct {
	organizationUrls map[string]string
	locations        map[string]model.InsertTmpDatasetLocationParams
	skills           map[string]struct{}
	degrees          map[string]struct{}
	studyFields      map[string]struct{}
	specialties      map[string]struct{}
	industries       map[string]struct{}
}

type Process struct {
	client       *apify.ApifyClient
	networkId    int64
	dbw          *dbw.DBW
	startedAt    int64
	idMap        dbw.IDSequenceMap
	lastPersonID int64
	aggregate    aggregate
	extract      extract
}

func InitProcess(dbw *dbw.DBW, token string, networkId int64) (*Process, error) {
	seqs, err := dbw.QuerySeqs(context.Background())
	if err != nil {
		return nil, err
	}

	p := Process{
		client:    apify.NewApifyClient(token),
		networkId: networkId,
		dbw:       dbw,
		startedAt: time.Now().Unix(),
		idMap:     seqs,
	}

	return &p, nil
}

func (p *Process) Run(ctx context.Context, profileUrls []string) error {
	personResults, err := apify.RunActorAndGetResults(p.client, apify.LINKEDIN_PERSONS_ACTOR, profileUrls, []apify.Person{})
	if err != nil {
		return err
	}

	if len(personResults) == 0 {
		return nil
	}

	if err := CreateTmpTables(ctx, p.dbw); err != nil {
		return err
	}

	for _, v := range personResults {
		person := p.extractPerson(v)
		p.extract.Persons = append(p.extract.Persons, person)

		for _, e := range v.Experience {
			experience := p.extractExperience(e, person.Urn)
			p.extract.Experiences = append(p.extract.Experiences, experience)
		}

		for _, e := range v.Education {
			education := p.extractEducation(e, person.Urn)
			p.extract.Educations = append(p.extract.Educations, education)
		}
	}

	if _, err := database.BatchInsert(ctx, p.dbw.DB, database.BatchInsertDesc[model.InsertTmpPersonParams]{
		Query: model.InsertTmpPerson,
		Items: p.extract.Persons,
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

	if _, err := database.BatchInsert(ctx, p.dbw.DB, database.BatchInsertDesc[model.InsertTmpExperienceParams]{
		Query: model.InsertTmpExperience,
		Items: p.extract.Experiences,
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

	if _, err := database.BatchInsert(ctx, p.dbw.DB, database.BatchInsertDesc[model.InsertTmpEducationParams]{
		Query: model.InsertTmpEducation,
		Items: p.extract.Educations,
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

	newOrganizationUrls, err := p.filterNewOrganizationURLs(ctx)
	if err != nil {
		return err
	}

	organizationResults, err := apify.RunActorAndGetResults(p.client, apify.LINKEDIN_COMPANIES_ACTOR, newOrganizationUrls, []apify.Company{})
	if err != nil {
		return err
	}

	for _, v := range organizationResults {
		organization := p.extractOrganization(v)
		p.extract.Organizations = append(p.extract.Organizations, organization)
	}

	if _, err := database.BatchInsert(ctx, p.dbw.DB, database.BatchInsertDesc[model.InsertTmpOrganizationParams]{
		Query: model.InsertTmpOrganization,
		Items: p.extract.Organizations,
		ExtractFn: func(itop model.InsertTmpOrganizationParams) []interface{} {
			return []interface{}{
				itop.Name,
				itop.UniversalName,
				itop.Website,
				itop.ProfileUrl,
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

	if _, err := database.BatchInsert(ctx, p.dbw.DB, database.BatchInsertDesc[model.InsertTmpOrganizationIndustryParams]{
		Query: model.InsertTmpOrganizationIndustry,
		Items: p.extract.OrgIndustries,
		ExtractFn: func(itop model.InsertTmpOrganizationIndustryParams) []interface{} {
			return []interface{}{
				itop.OrganizationUrn,
				itop.Industry,
			}
		},
	}); err != nil {
		return err
	}

	if _, err := database.BatchInsert(ctx, p.dbw.DB, database.BatchInsertDesc[model.InsertTmpOrganizationSpecialtyParams]{
		Query: model.InsertTmpOrganizationSpecialty,
		Items: p.extract.OrgSpecialties,
		ExtractFn: func(itop model.InsertTmpOrganizationSpecialtyParams) []interface{} {
			return []interface{}{
				itop.OrganizationUrn,
				itop.Specialty,
			}
		},
	}); err != nil {
		return err
	}

	if _, err := database.BatchInsert(ctx, p.dbw.DB, database.BatchInsertDesc[model.InsertTmpOrganizationLocationParams]{
		Query: model.InsertTmpOrganizationLocation,
		Items: p.extract.OrgLocations,
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

	var degrees []string
	for k := range p.aggregate.degrees {
		degrees = append(degrees, k)
	}

	if _, err := database.BatchInsert(ctx, p.dbw.DB, database.BatchInsertDesc[string]{
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
	for k := range p.aggregate.industries {
		industries = append(industries, k)
	}

	if _, err := database.BatchInsert(ctx, p.dbw.DB, database.BatchInsertDesc[string]{
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
	for _, l := range p.aggregate.locations {
		locations = append(locations, l)
	}

	if _, err := database.BatchInsert(ctx, p.dbw.DB, database.BatchInsertDesc[model.InsertTmpDatasetLocationParams]{
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
	for k := range p.aggregate.skills {
		skills = append(skills, k)
	}

	if _, err := database.BatchInsert(ctx, p.dbw.DB, database.BatchInsertDesc[string]{
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
	for k := range p.aggregate.specialties {
		specialties = append(specialties, k)
	}

	if _, err := database.BatchInsert(ctx, p.dbw.DB, database.BatchInsertDesc[string]{
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
	for k := range p.aggregate.studyFields {
		studyFields = append(studyFields, k)
	}

	if _, err := database.BatchInsert(ctx, p.dbw.DB, database.BatchInsertDesc[string]{
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

	if err := p.InsertDatasetDataFromTmp(ctx); err != nil {
		return err
	}

	if err := p.InsertOrganizationDataFromTmp(ctx); err != nil {
		return err
	}

	if err := p.InsertPersonDataFromTmp(ctx); err != nil {
		return err
	}

	return nil
}

func (p *Process) InsertDatasetDataFromTmp(ctx context.Context) error {
	tx, err := p.dbw.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("BeginTx %w", err)
	}
	qtx := p.dbw.SQLC.WithTx(tx)

	err = qtx.InsertDatasetDegreesFromTmp(ctx)
	if err != nil {
		return fmt.Errorf("InsertDatasetDegreesFromTmp %w", err)
	}

	err = qtx.InsertDatasetIndustriesFromTmp(ctx)
	if err != nil {
		return fmt.Errorf("InsertDatasetIndustriesFromTmp %w", err)
	}

	err = qtx.InsertDatasetLocationsFromTmp(ctx)
	if err != nil {
		return fmt.Errorf("InsertDatasetLocationsFromTmp %w", err)
	}

	err = qtx.InsertDatasetSkillsFromTmp(ctx)
	if err != nil {
		return fmt.Errorf("InsertDatasetSkillsFromTmp %w", err)
	}

	err = qtx.InsertDatasetSpecialtiesFromTmp(ctx)
	if err != nil {
		return fmt.Errorf("InsertDatasetSpecialtiesFromTmp %w", err)
	}

	err = qtx.InsertDatasetStudyFieldsFromTmp(ctx)
	if err != nil {
		return fmt.Errorf("InsertDatasetStudyFieldsFromTmp %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("tx.Commit %w", err)
	}

	return nil
}

func (p *Process) InsertOrganizationDataFromTmp(ctx context.Context) error {
	tx, err := p.dbw.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("BeginTx %w", err)
	}
	qtx := p.dbw.SQLC.WithTx(tx)

	err = qtx.InsertOrganizationsFromTmp(ctx)
	if err != nil {
		return fmt.Errorf("InsertOrganizationsFromTmp %w", err)
	}

	err = qtx.InsertOrganizationLocationsFromTmp(ctx)
	if err != nil {
		return fmt.Errorf("InsertOrganizationLocationsFromTmp %w", err)
	}

	err = qtx.InsertOrganizationIndustriesFromTmp(ctx)
	if err != nil {
		return fmt.Errorf("InsertOrganizationIndustriesFromTmp %w", err)
	}

	err = qtx.InsertOrganizationSpecialtiesFromTmp(ctx)
	if err != nil {
		return fmt.Errorf("InsertOrganizationSpecialtiesFromTmp %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("tx.Commit %w", err)
	}

	return nil
}

func (p *Process) InsertPersonDataFromTmp(ctx context.Context) error {
	tx, err := p.dbw.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("BeginTx %w", err)
	}
	qtx := p.dbw.SQLC.WithTx(tx)

	err = qtx.InsertPersonsFromTmp(ctx)
	if err != nil {
		return fmt.Errorf("InsertPersonsFromTmp %w", err)
	}

	err = qtx.InsertPersonSkillsFromTmp(ctx)
	if err != nil {
		return fmt.Errorf("InsertPersonSkillsFromTmp %w", err)
	}

	err = qtx.InsertExperiencesFromTmp(ctx)
	if err != nil {
		return fmt.Errorf("InsertExperiencesFromTmp %w", err)
	}

	err = qtx.InsertEducationsFromTmp(ctx)
	if err != nil {
		return fmt.Errorf("InsertEducationsFromTmp %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("tx.Commit %w", err)
	}

	return nil
}

func (p *Process) extractPerson(v apify.Person) model.InsertTmpPersonParams {
	p.lastPersonID += 1

	person := model.InsertTmpPersonParams{
		FirstName:        v.BasicInfo.FirstName,
		LastName:         v.BasicInfo.LastName,
		ProfileUrl:       v.BasicInfo.ProfileURL,
		PublicIdentifier: v.BasicInfo.PublicIdentifer,
		Urn:              v.BasicInfo.URN,
		CreatedAt:        p.startedAt,
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

	if len(v.BasicInfo.Location.City) > 0 || len(v.BasicInfo.Location.Country) > 0 {
		name := strings.Join([]string{v.BasicInfo.Location.City, v.BasicInfo.Location.Country}, ", ")
		_, ok := p.aggregate.locations[v.BasicInfo.Location.Full]
		if !ok {
			location := model.InsertTmpDatasetLocationParams{
				Name: name,
			}

			if len(v.BasicInfo.Location.City) > 0 {
				location.City = &v.BasicInfo.Location.City
			}

			if len(v.BasicInfo.Location.Country) > 0 {
				location.Country = &v.BasicInfo.Location.Country
			}

			p.aggregate.locations[v.BasicInfo.Location.Full] = location
		}
	}

	return person
}

func (p *Process) extractExperience(e apify.Experience, personUrn string) model.InsertTmpExperienceParams {
	_, ok := p.aggregate.organizationUrls[e.CompanyID]
	if !ok {
		p.aggregate.organizationUrls[e.CompanyID] = e.CompanyLinkedinURL
	}

	exp := model.InsertTmpExperienceParams{
		PersonUrn:       personUrn,
		OrganizationUrn: e.CompanyID,
		Title:           e.Title,
	}

	for _, s := range e.Skills {
		if _, ok := p.aggregate.skills[s]; !ok {
			p.aggregate.skills[s] = struct{}{}
		}
		p.extract.PersonSkills = append(p.extract.PersonSkills, model.InsertTmpPersonSkillParams{
			PersonUrn: personUrn,
			Skill:     s,
		})
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

	return exp
}

func (p *Process) extractEducation(e apify.Education, personUrn string) model.InsertTmpEducationParams {
	_, ok := p.aggregate.organizationUrls[e.SchoolID]
	if !ok {
		p.aggregate.organizationUrls[e.SchoolID] = e.SchoolLinkedinURL
	}

	edu := model.InsertTmpEducationParams{
		PersonUrn: personUrn,
	}

	if len(e.Degree) > 0 {
		edu.Degree = &e.Degree
		if _, ok := p.aggregate.degrees[e.Degree]; !ok {
			p.aggregate.degrees[e.Degree] = struct{}{}
		}
	}

	if len(e.FieldOfStudy) > 0 {
		edu.StudyField = &e.FieldOfStudy
		if _, ok := p.aggregate.studyFields[e.FieldOfStudy]; !ok {
			p.aggregate.studyFields[e.FieldOfStudy] = struct{}{}
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

	return edu
}

func (p *Process) filterNewOrganizationURLs(ctx context.Context) ([]string, error) {
	var profileUrls []string
	for _, v := range p.aggregate.organizationUrls {
		profileUrls = append(profileUrls, v)
	}

	existing, err := p.dbw.SQLC.SelectOrganizationsByLinkedinURLs(ctx, profileUrls)
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

func (p *Process) extractOrganization(v apify.Company) model.InsertTmpOrganizationParams {
	org := model.InsertTmpOrganizationParams{
		Name:          v.BasicInfo.Name,
		UniversalName: v.BasicInfo.UniversalName,
		ProfileUrl:    v.BasicInfo.LinkedinURL,
		Urn:           v.CompanyURN,
		CreatedAt:     p.startedAt,
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

	for _, s := range v.BasicInfo.Specialties {
		if _, ok := p.aggregate.specialties[s]; !ok {
			p.aggregate.specialties[s] = struct{}{}
		}
		p.extract.OrgSpecialties = append(p.extract.OrgSpecialties, model.InsertTmpOrganizationSpecialtyParams{
			OrganizationUrn: v.CompanyURN,
			Specialty:       s,
		})
	}

	for _, s := range v.BasicInfo.Industries {
		if _, ok := p.aggregate.industries[s]; !ok {
			p.aggregate.industries[s] = struct{}{}
		}
		p.extract.OrgIndustries = append(p.extract.OrgIndustries, model.InsertTmpOrganizationIndustryParams{
			OrganizationUrn: v.CompanyURN,
			Industry:        s,
		})
	}

	if len(v.Locations.Headquarters.City) > 0 || len(v.Locations.Headquarters.State) > 0 || len(v.Locations.Headquarters.Country) > 0 {
		name := strings.Join([]string{v.Locations.Headquarters.City, v.Locations.Headquarters.State, v.Locations.Headquarters.Country}, ", ")
		_, ok := p.aggregate.locations[name]
		if !ok {
			location := model.InsertTmpDatasetLocationParams{}

			if len(v.Locations.Headquarters.City) > 0 {
				location.City = &v.Locations.Headquarters.City
			}

			if len(v.Locations.Headquarters.State) > 0 {
				location.State = &v.Locations.Headquarters.State
			}

			if len(v.Locations.Headquarters.Country) > 0 {
				location.Country = &v.Locations.Headquarters.Country
			}

			p.aggregate.locations[name] = location
			p.extract.OrgLocations = append(p.extract.OrgLocations, model.InsertTmpOrganizationLocationParams{
				OrganizationUrn: v.CompanyURN,
				Location:        name,
				IsHeadquarters:  1,
			})
		}
	}

	for _, l := range v.Locations.Offices {
		name := strings.Join([]string{l.City, l.State, l.Country}, ", ")
		_, ok := p.aggregate.locations[name]
		if !ok {
			location := model.InsertTmpDatasetLocationParams{}

			if len(l.City) > 0 {
				location.City = &l.City
			}

			if len(l.State) > 0 {
				location.State = &l.State
			}

			if len(l.Country) > 0 {
				location.Country = &l.Country
			}

			p.aggregate.locations[name] = location
			p.extract.OrgLocations = append(p.extract.OrgLocations, model.InsertTmpOrganizationLocationParams{
				OrganizationUrn: v.CompanyURN,
				Location:        name,
				IsHeadquarters:  0,
			})
		}
	}

	return org
}
