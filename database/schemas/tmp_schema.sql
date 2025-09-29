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
  study_field TEXT,
  start_year INTEGER,
  start_month INTEGER,
  end_year INTEGER,
  end_month INTEGER
);