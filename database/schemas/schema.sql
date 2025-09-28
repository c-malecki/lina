-- name: CreateDatasetDegreesTable :exec
CREATE TABLE IF NOT EXISTS dataset_degrees (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  "name" TEXT NOT NULL UNIQUE
);

-- name: CreateDatasetIndustriesTable :exec
CREATE TABLE IF NOT EXISTS dataset_industries (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  "name" TEXT NOT NULL UNIQUE
);

-- name: CreateDatasetLocationsTable :exec
CREATE TABLE IF NOT EXISTS dataset_locations (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  "name" TEXT NOT NULL UNIQUE,
  city TEXT,
  "state" TEXT,
  country TEXT,
  country_code TEXT,
  UNIQUE (city, "state", country)
);

-- name: CreateDatasetSkillsTable :exec
CREATE TABLE IF NOT EXISTS dataset_skills (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  "name" TEXT NOT NULL UNIQUE
);

-- name: CreateDatasetSpecialtiesTable :exec
CREATE TABLE IF NOT EXISTS dataset_specialties (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  "name" TEXT NOT NULL UNIQUE
);

-- name: CreateDatasetStudyFieldsTable :exec
CREATE TABLE IF NOT EXISTS dataset_study_fields (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  "name" TEXT NOT NULL UNIQUE
);

-- name: CreateUsersTable :exec
CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  username TEXT NOT NULL,
  "password" TEXT NOT NULL,
  apify_token TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER
);

-- name: CreateOrganizationsTable :exec
CREATE TABLE IF NOT EXISTS organizations (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  "name" TEXT NOT NULL,
  universal_name TEXT NOT NULL,
  website TEXT,
  linkedin_url TEXT NOT NULL,
  logo_url TEXT,
  founded_year INTEGER,
  founded_month TEXT,
  organization_type INTEGER NOT NULL DEFAULT 0,
  employee_count INTEGER,
  student_count INTEGER,
  urn TEXT NOT NULL UNIQUE,
  created_at INTEGER NOT NULL,
  updated_at INTEGER
);

-- name: CreateOrganizationLocationsTable :exec
CREATE TABLE IF NOT EXISTS organization_locations (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  organization_id INTEGER NOT NULL,
  location_id INTEGER NOT NULL,
  is_headquarters INTEGER NOT NULL DEFAULT 0,
  UNIQUE (organization_id, location_id),
  FOREIGN KEY (organization_id) REFERENCES organizations(id) ON DELETE CASCADE,
  FOREIGN KEY (location_id) REFERENCES dataset_locations(id) ON DELETE CASCADE
);

-- name: CreateOrganizationSpecialtiesTable :exec
CREATE TABLE IF NOT EXISTS organization_specialties (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  organization_id INTEGER NOT NULL,
  specialty_id INTEGER NOT NULL,
  UNIQUE (organization_id, specialty_id),
  FOREIGN KEY (organization_id) REFERENCES organizations(id) ON DELETE CASCADE,
  FOREIGN KEY (specialty_id) REFERENCES dataset_specialties(id) ON DELETE CASCADE
);

-- name: CreateOrganizationIndustriesTable :exec
CREATE TABLE IF NOT EXISTS organization_industries (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  organization_id INTEGER NOT NULL,
  industry_id INTEGER NOT NULL,
  UNIQUE (organization_id, industry_id),
  FOREIGN KEY (organization_id) REFERENCES organizations(id) ON DELETE CASCADE,
  FOREIGN KEY (industry_id) REFERENCES dataset_industries(id) ON DELETE CASCADE
);

-- name: CreatePersonsTable :exec
CREATE TABLE IF NOT EXISTS persons (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  first_name TEXT NOT NULL,
  last_name TEXT NOT NULL,
  headline TEXT,
  profile_url TEXT NOT NULL,
  public_identifier TEXT NOT NULL,
  profile_picture_url TEXT,
  about TEXT,
  location_id INTEGER,
  urn TEXT NOT NULL UNIQUE,
  current_company_id INTEGER,
  created_at INTEGER NOT NULL,
  updated_at INTEGER,
  FOREIGN KEY (location_id) REFERENCES dataset_locations(id) ON DELETE SET NULL,
  FOREIGN KEY (current_company_id) REFERENCES organizations(id) ON DELETE SET NULL
);

-- name: CreateNetworksTable :exec
CREATE TABLE IF NOT EXISTS networks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  "name" TEXT NOT NULL,
  updated_at INTEGER,
  UNIQUE (user_id, "name"),
  FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

-- name: CreateNetworkConnectionsTable :exec
CREATE TABLE IF NOT EXISTS network_connections (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  network_id INTEGER NOT NULL,
  person_id INTEGER NOT NULL,
  UNIQUE (network_id, person_id),
  FOREIGN KEY (network_id) REFERENCES networks(id) ON DELETE CASCADE,
  FOREIGN KEY (person_id) REFERENCES persons(id) ON DELETE CASCADE
);

-- name: CreateExperiencesTable :exec
CREATE TABLE IF NOT EXISTS experiences (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  person_id INTEGER NOT NULL,
  organization_id INTEGER NOT NULL,
  title TEXT NOT NULL,
  location_raw TEXT,
  "description" TEXT,
  start_year INTEGER,
  start_month TEXT,
  is_current INTEGER NOT NULL,
  end_year INTEGER,
  end_month TEXT,
  skills_url TEXT,
  FOREIGN KEY (person_id) REFERENCES persons(id) ON DELETE CASCADE,
  FOREIGN KEY (organization_id) REFERENCES organizations(id) ON DELETE CASCADE
);

-- name: CreateEducationsTable :exec
CREATE TABLE IF NOT EXISTS educations (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  person_id INTEGER NOT NULL,
  organization_id INTEGER NOT NULL,
  degree_id INTEGER NOT NULL,
  study_field_id INTEGER NOT NULL,
  "description" TEXT,
  start_year INTEGER,
  start_month TEXT,
  end_year INTEGER,
  end_month TEXT,
  FOREIGN KEY (person_id) REFERENCES persons(id) ON DELETE CASCADE,
  FOREIGN KEY (organization_id) REFERENCES organizations(id) ON DELETE CASCADE,
  FOREIGN KEY (degree_id) REFERENCES dataset_degrees(id) ON DELETE SET NULL,
  FOREIGN KEY (study_field_id) REFERENCES dataset_study_fields(id) ON DELETE SET NULL
);