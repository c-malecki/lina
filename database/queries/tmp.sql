-- name: CreateTmpDatasetDegreesTable :exec
CREATE TEMPORARY TABLE IF NOT EXISTS tmp_dataset_degrees (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  "name" TEXT NOT NULL UNIQUE
);

-- name: CreateTmpDatasetIndustriesTable :exec
CREATE TEMPORARY TABLE IF NOT EXISTS tmp_dataset_industries (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  "name" TEXT NOT NULL UNIQUE
);

-- name: CreateTmpDatasetLocationsTable :exec
CREATE TEMPORARY TABLE IF NOT EXISTS tmp_dataset_locations (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  "name" TEXT NOT NULL UNIQUE,
  city TEXT,
  "state" TEXT,
  country TEXT,
  UNIQUE (city, "state", country)
);

-- name: CreateTmpDatasetSkillsTable :exec
CREATE TEMPORARY TABLE IF NOT EXISTS tmp_dataset_skills (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  "name" TEXT NOT NULL UNIQUE
);

-- name: CreateTmpDatasetSpecialtiesTable :exec
CREATE TEMPORARY TABLE IF NOT EXISTS tmp_dataset_specialties (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  "name" TEXT NOT NULL UNIQUE
);

-- name: CreateTmpDatasetStudyFieldsTable :exec
CREATE TEMPORARY TABLE IF NOT EXISTS tmp_dataset_study_fields (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  "name" TEXT NOT NULL UNIQUE
);

-- name: CreateTmpOrganizationsTable :exec
CREATE TEMPORARY TABLE IF NOT EXISTS tmp_organizations (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  "name" TEXT NOT NULL,
  universal_name TEXT NOT NULL,
  website TEXT,
  profile_url TEXT NOT NULL,
  logo_url TEXT,
  founded_year INTEGER,
  founded_month INTEGER,
  organization_type INTEGER NOT NULL DEFAULT 0,
  employee_count INTEGER,
  student_count INTEGER,
  urn TEXT NOT NULL UNIQUE,
  created_at INTEGER NOT NULL
);

-- name: CreateTmpOrganizationLocationsTable :exec
CREATE TEMPORARY TABLE IF NOT EXISTS tmp_organization_locations (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  organization_urn TEXT NOT NULL,
  "location" TEXT NOT NULL,
  is_headquarters INTEGER NOT NULL DEFAULT 0,
  UNIQUE (organization_urn, "location")
);

-- name: CreateTmpOrganizationSpecialtiesTable :exec
CREATE TEMPORARY TABLE IF NOT EXISTS tmp_organization_specialties (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  organization_urn TEXT NOT NULL,
  specialty TEXT NOT NULL,
  UNIQUE (organization_urn, specialty)
);

-- name: CreateTmpOrganizationIndustriesTable :exec
CREATE TEMPORARY TABLE IF NOT EXISTS tmp_organization_industries (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  organization_urn TEXT NOT NULL,
  industry TEXT NOT NULL,
  UNIQUE (organization_urn, industry)
);

-- name: CreateTmpPersonsTable :exec
CREATE TEMPORARY TABLE IF NOT EXISTS tmp_persons (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  first_name TEXT NOT NULL,
  last_name TEXT NOT NULL,
  headline TEXT,
  profile_url TEXT NOT NULL,
  public_identifier TEXT NOT NULL,
  profile_picture_url TEXT,
  about TEXT,
  "location" TEXT,
  urn TEXT NOT NULL UNIQUE,
  created_at INTEGER NOT NULL
);

-- name: CreateTmpPersonSkillsTable :exec
CREATE TEMPORARY TABLE IF NOT EXISTS tmp_person_skills (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  person_urn TEXT NOT NULL,
  skill TEXT NOT NULL,
  UNIQUE (person_id, skill_id)
);

-- name: CreateTmpNetworkConnectionsTable :exec
CREATE TEMPORARY TABLE IF NOT EXISTS tmp_network_connections (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  network_id INTEGER NOT NULL,
  person_id INTEGER NOT NULL,
  UNIQUE (network_id, person_id)
);

-- name: CreateTmpExperiencesTable :exec
CREATE TEMPORARY TABLE IF NOT EXISTS tmp_experiences (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  person_urn TEXT NOT NULL,
  organization_urn TEXT NOT NULL,
  title TEXT NOT NULL,
  location_raw TEXT,
  "description" TEXT,
  start_year INTEGER,
  start_month INTEGER,
  is_current INTEGER NOT NULL,
  end_year INTEGER,
  end_month INTEGER,
  skills_url TEXT
);

-- name: CreateTmpEducationsTable :exec
CREATE TEMPORARY TABLE IF NOT EXISTS tmp_educations (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  person_urn TEXT NOT NULL,
  organization_urn TEXT NOT NULL,
  degree TEXT,
  study TEXT,
  start_year INTEGER,
  start_month INTEGER,
  end_year INTEGER,
  end_month INTEGER
);

-- name: InsertTmpDatasetDegree :exec
INSERT INTO tmp_dataset_degrees ("name") VALUES (?);

-- name: InsertTmpDatasetIndustry :exec
INSERT INTO tmp_dataset_industries ("name") VALUES (?);

-- name: InsertTmpDatasetLocation :exec
INSERT INTO tmp_dataset_locations
("name", city, "state", country)
VALUES
(?, ?, ?, ?);

-- name: InsertTmpDatasetSkill :exec
INSERT INTO tmp_dataset_skills ("name") VALUES (?);

-- name: InsertTmpDatasetSpecialty :exec
INSERT INTO tmp_dataset_specialties ("name") VALUES (?);

-- name: InsertTmpDatasetStudyField :exec
INSERT INTO tmp_dataset_study_fields ("name") VALUES (?);

-- name: InsertTmpOrganization :exec
INSERT INTO tmp_organizations
("name", universal_name, website, profile_url, logo_url, founded_year, founded_month, organization_type, employee_count, student_count, urn, created_at)
VALUES
(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

-- name: InsertTmpOrganizationLocation :exec
INSERT INTO tmp_organization_locations
(organization_urn, "location", is_headquarters)
VALUES
(?, ?, ?);

-- name: InsertTmpOrganizationSpecialty :exec
INSERT INTO tmp_organization_specialties
(organization_urn, specialty)
VALUES
(?, ?);

-- name: InsertTmpOrganizationIndustry :exec
INSERT INTO tmp_organization_industries
(organization_urn, industry)
VALUES
(?, ?);

-- name: InsertTmpPerson :exec
INSERT INTO tmp_persons
(first_name, last_name, headline, profile_url, public_identifier, profile_picture_url, about, "location", urn, created_at)
VALUES
(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

-- name: InsertTmpPersonSkill :exec
INSERT INTO tmp_person_skills
(person_urn, skill)
VALUES
(?, ?);

-- name: InsertTmpExperience :exec
INSERT INTO tmp_experiences
(person_urn, organization_urn, title, location_raw, "description", start_year, start_month, is_current, end_year, end_month, skills_url)
VALUES
(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

-- name: InsertTmpEducation :exec
INSERT INTO tmp_educations
(person_urn, organization_urn, degree, study_field, start_year, start_month, end_year, end_month)
VALUES
(?, ?, ?, ?, ?, ?, ?, ?);

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
LEFT JOIN organizations o ON o.urn = t.urn
WHERE o.id IS NULL;

-- name: InsertOrganizationLocationsFromTmp :exec
INSERT INTO organization_locations
(organization_id, location_id, is_headquarters)
SELECT
  o.id AS organization_id,
  d.id AS location_id,
  t.is_headquarters
FROM tmp_organization_locations t
INNER JOIN organizations o ON o.urn = t.organization_urn
INNER JOIN dataset_locations d ON d.name = t.location;

-- name: InsertOrganizationSpecialtiesFromTmp :exec
INSERT INTO organization_specialties
(organization_id, specialty_id)
SELECT
  o.id AS organization_id,
  d.id AS specialty_id
FROM tmp_organization_specialties t
INNER JOIN organizations o ON o.urn = t.organization_urn
INNER JOIN dataset_specialties d ON d.name = t.specialty;

-- name: InsertOrganizationIndustriesFromTmp :exec
INSERT INTO organization_industries
(organization_id, industry_id)
SELECT
  o.id AS organization_id,
  d.id AS industry_id
FROM tmp_organization_industries t
INNER JOIN organizations o ON o.urn = t.organization_urn
INNER JOIN dataset_industries d ON d.name = t.industry;

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
LEFT JOIN dataset_locations dl ON dl.name = t.location;

-- name: InsertPersonSkillsFromTmp :exec
INSERT INTO person_skills
(person_id, skill_id)
SELECT
  p.id AS person_id,
  d.id AS skill
FROM tmp_person_skills t
INNER JOIN persons p ON p.urn = t.person_urn
INNER JOIN dataset_skills d ON d.name = t.skill;

-- name: InsertExperiencesFromTmp :exec
INSERT INTO experiences
(person_id, organization_id, title, location_raw, "description", start_year, start_month, is_current, end_year, end_month, skills_url)
SELECT
  p.id AS person_id,
  o.id AS organization_id,
  t.title,
  t.description,
  t.start_year,
  t.start_month,
  t.is_current,
  t.end_year,
  t.end_month,
  t.skills_url
FROM tmp_experiences t
INNER JOIN persons p ON p.urn = t.person_urn
INNER JOIN organizations o ON o.urn = t.organization_urn;

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
LEFT JOIN dataset_study_fields dsf ON dsf.name = t.study_field;