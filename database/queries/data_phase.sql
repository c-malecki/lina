-- datasets

-- name: InsertDatasetDegreesFromTmp :exec
INSERT INTO dataset_degrees
("name")
SELECT
  t.name
FROM tmp_dataset_degrees t
LEFT JOIN dataset_degrees d ON d.name = t.name
WHERE d.id IS NULL;

-- name: InsertDatasetIndustriesFromTmp :exec
INSERT INTO dataset_industries
("name")
SELECT
  t.name
FROM tmp_dataset_industries t
LEFT JOIN dataset_industries d ON d.name = t.name
WHERE d.id IS NULL;

-- name: InsertDatasetLocationsFromTmp :exec
INSERT INTO dataset_locations
("name", city, "state", country)
SELECT
  t.name,
  t.city,
  t.state,
  t.country
FROM tmp_dataset_locations t
LEFT JOIN dataset_locations d ON d.name = t.name
WHERE d.id IS NULL;

-- name: InsertDatasetSkillsFromTmp :exec
INSERT INTO dataset_skills
("name")
SELECT
  t.name
FROM tmp_dataset_skills t
LEFT JOIN dataset_skills d ON d.name = t.name
WHERE d.id IS NULL;

-- name: InsertDatasetSpecialtiesFromTmp :exec
INSERT INTO dataset_specialties
("name")
SELECT
  t.name
FROM tmp_dataset_specialties t
LEFT JOIN dataset_specialties d ON d.name = t.name
WHERE d.id IS NULL;

-- name: InsertDatasetStudyFieldsFromTmp :exec
INSERT INTO dataset_study_fields
("name")
SELECT
  t.name
FROM tmp_dataset_study_fields t
LEFT JOIN dataset_study_fields d ON d.name = t.name
WHERE d.id IS NULL;

-- organizations

-- name: InsertOrganizationsFromTmp :exec
INSERT INTO organizations
("name", universal_name, website, profile_url, logo_url, founded_year, founded_month, organization_type, employee_count, student_count, urn, created_at)
SELECT
  t.name,
  t.universal_name,
  t.website,
  t.profile_url,
  t.logo_url,
  t.founded_year,
  t.founded_month,
  t.organization_type,
  t.employee_count,
  t.student_count,
  t.urn,
  t.created_at
FROM tmp_organizations t
LEFT JOIN organizations existing ON existing.urn = t.urn
WHERE existing.id IS NULL;

-- name: InsertOrganizationLocationsFromTmp :exec
INSERT INTO organization_locations
(organization_id, location_id, is_headquarters)
SELECT
  o.id AS organization_id,
  d.id AS location_id,
  t.is_headquarters
FROM tmp_organization_locations t
INNER JOIN organizations o ON o.urn = t.organization_urn
INNER JOIN dataset_locations d ON d.name = t.location
LEFT JOIN organization_locations existing
  ON existing.organization_id = o.id
  AND existing.location_id = d.id
  AND existing.is_headquarters = t.is_headquarters
WHERE existing.id IS NULL;

-- name: InsertOrganizationIndustriesFromTmp :exec
INSERT INTO organization_industries
(organization_id, industry_id)
SELECT
  o.id AS organization_id,
  d.id AS industry_id
FROM tmp_organization_industries t
INNER JOIN organizations o ON o.urn = t.organization_urn
INNER JOIN dataset_industries d ON d.name = t.industry
LEFT JOIN organization_industries existing
  ON existing.organization_id = o.id
  AND existing.industry_id = d.id
WHERE existing.id IS NULL;

-- name: InsertOrganizationSpecialtiesFromTmp :exec
INSERT INTO organization_specialties
(organization_id, specialty_id)
SELECT
  o.id AS organization_id,
  d.id AS specialty_id
FROM tmp_organization_specialties t
INNER JOIN organizations o ON o.urn = t.organization_urn
INNER JOIN dataset_specialties d ON d.name = t.specialty
LEFT JOIN organization_specialties existing
  ON existing.organization_id = o.id
  AND existing.specialty_id = d.id
WHERE existing.id IS NULL;

-- persons

-- name: InsertPersonsFromTmp :exec
INSERT INTO persons
(first_name, last_name, headline, profile_url, profile_picture_url, public_identifier, about, location_id, urn, created_at)
SELECT
  t.first_name,
  t.last_name,
  t.headline,
  t.profile_url,
  t.profile_picture_url,
  t.public_identifier,
  t.about,
  dl.id AS location_id,
  t.urn,
  t.created_at
FROM tmp_persons t
LEFT JOIN dataset_locations dl ON dl.name = t.location
LEFT JOIN persons existing
  ON existing.urn = t.urn
WHERE existing.id IS NULL;

-- name: InsertPersonSkillsFromTmp :exec
INSERT INTO person_skills
(person_id, skill_id)
SELECT
  p.id AS person_id,
  d.id AS skill_id
FROM tmp_person_skills t
INNER JOIN persons p ON p.urn = t.person_urn
INNER JOIN dataset_skills d ON d.name = t.skill
LEFT JOIN person_skills existing
  ON existing.person_id = p.id
  AND existing.skill_id = d.id
WHERE existing.id IS NULL;

-- name: InsertExperiencesFromTmp :exec
INSERT INTO experiences
(person_id, organization_id, title, location_raw, "description", start_year, start_month, is_current, end_year, end_month, skills_url)
SELECT
  p.id AS person_id,
  o.id AS organization_id,
  t.title,
  t.location_raw,
  t.description,
  t.start_year,
  t.start_month,
  t.is_current,
  t.end_year,
  t.end_month,
  t.skills_url
FROM tmp_experiences t
INNER JOIN persons p ON p.urn = t.person_urn
INNER JOIN organizations o ON o.urn = t.organization_urn
LEFT JOIN experiences existing
  ON existing.person_id = p.id
  AND existing.organization_id = o.id
  AND existing.title = t.title
WHERE existing.id IS NULL;

-- name: InsertEducationsFromTmp :exec
INSERT INTO educations
(person_id, organization_id, degree_id, study_field_id, start_year, start_month, end_year, end_month)
SELECT
  p.id AS person_id,
  o.id AS organization_id,
  dd.id AS degree_id,
  dsf.id AS study_field_id,
  t.start_year,
  t.start_month,
  t.end_year,
  t.end_month
FROM tmp_educations t
INNER JOIN persons p ON p.urn = t.person_urn
INNER JOIN organizations o ON o.urn = t.organization_urn
LEFT JOIN dataset_degrees dd ON dd.name = t.degree
LEFT JOIN dataset_study_fields dsf ON dsf.name = t.study_field
LEFT JOIN educations existing
  ON existing.person_id = p.id
  AND existing.organization_id = o.id
  AND existing.start_year = t.start_year
  AND existing.start_month = t.start_month
WHERE existing.id IS NULL;

-- testing

-- name: CountTmpConnections :one
SELECT COUNT(*) FROM tmp_connections;

-- name: CountTmpDatasetDegrees :one
SELECT COUNT(*) FROM tmp_dataset_degrees;

-- name: CountTmpDatasetStudyFields :one
SELECT COUNT(*) FROM tmp_dataset_study_fields;

-- name: CountTmpDatasetIndustries :one
SELECT COUNT(*) FROM tmp_dataset_industries;

-- name: CountTmpDatasetSpecialies :one
SELECT COUNT(*) FROM tmp_dataset_specialties;

-- name: CountTmpDatasetSkills :one
SELECT COUNT(*) FROM tmp_dataset_skills;

-- name: CountTmpDatasetLocations :one
SELECT COUNT(*) FROM tmp_dataset_locations;

-- name: CountTmpPersons :one
SELECT COUNT(*) FROM tmp_persons;

-- name: CountTmpPersonSkills :one
SELECT COUNT(*) FROM tmp_person_skills;

-- name: CountTmpExperiences :one
SELECT COUNT(*) FROM tmp_experiences;

-- name: CountTmpEducations :one
SELECT COUNT(*) FROM tmp_educations;

-- name: CountTmpOrganizations :one
SELECT COUNT(*) FROM tmp_organizations;

-- name: CountTmpCompanies :one
SELECT COUNT(*) FROM tmp_organizations WHERE organization_type = 1;

-- name: CountTmpSchools :one
SELECT COUNT(*) FROM tmp_organizations WHERE organization_type = 2;

-- name: CountTmpOrganizationIndustries :one
SELECT COUNT(*) FROM tmp_organization_industries;

-- name: CountTmpOrganizationSpecialties :one
SELECT COUNT(*) FROM tmp_organization_specialties;

-- name: CountTmpOrganizationLocations :one
SELECT COUNT(*) FROM tmp_organization_locations;

-- name: SelectTestTmpExperiences :many
SELECT e.*, t.name AS organization_name
FROM tmp_experiences e
INNER JOIN tmp_organizations t ON t.urn = e.organization_urn;

-- name: SelectTestExperiences :many
SELECT
  e.id,
  p.urn AS person_urn,
  o.urn AS organization_urn,
  o.name AS organization_name,
  e.title,
  e.location_raw,
  e.start_year,
  e.start_month,
  e.is_current,
  e.end_year,
  e.end_month,
  e.skills_url
FROM experiences e
INNER JOIN persons p ON p.id = e.person_id
INNER JOIN organizations o ON o.id = e.organization_id;

-- name: SelectTestTmpEducations :many
SELECT e.*, t.name AS organization_name
FROM tmp_educations e
INNER JOIN tmp_organizations t ON t.urn = e.organization_urn;

-- name: SelectTestEducations :many
SELECT
  e.id,
  p.urn AS person_urn,
  o.urn AS organization_urn,
  o.name AS organization_name,
  dd.name AS degree,
  dsf.name AS study_field,
  e.start_year,
  e.start_month,
  e.end_year,
  e.end_month
FROM educations e
INNER JOIN persons p ON p.id = e.person_id
INNER JOIN organizations o ON o.id = e.organization_id
LEFT JOIN dataset_degrees dd ON dd.id = e.degree_id
LEFT JOIN dataset_study_fields dsf ON dsf.id = e.study_field_id;

-- name: SelectTestCompanies :many
SELECT * FROM organizations WHERE organization_type = 1;

-- name: SelectTestSchools :many
SELECT * FROM organizations WHERE organization_type = 2;

-- name: SelectTestTmpConnections :many
SELECT * FROM tmp_connections;

-- name: SelectTestTmpPersons :many
SELECT * FROM tmp_persons;

-- name: SelectTestPersons :many
SELECT * FROM persons;