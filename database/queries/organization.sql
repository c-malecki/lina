-- name: SelectOrganizationsByLinkedinURLs :many
SELECT id, profile_url FROM organizations WHERE profile_url IN (sqlc.slice(linkedin_urls));

-- name: InsertOrganization :exec
INSERT INTO organizations
("name", universal_name, website, profile_url, logo_url, founded_year, founded_month, organization_type, employee_count, student_count, urn, created_at)
VALUES
(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

-- name: InsertOrganizationLocation :exec
INSERT INTO organization_locations
(organization_id, location_id, is_headquarters)
VALUES
(?, ?, ?);

-- name: InsertOrganizationSpecialty :exec
INSERT INTO organization_specialties
(organization_id, specialty_id)
VALUES
(?, ?);

-- name: InsertOrganizationIndustry :exec
INSERT INTO organization_industries
(organization_id, industry_id)
VALUES
(?, ?);

-- name: CountOrganizations :one
SELECT COUNT(*) FROM organizations;

-- name: CountCompanies :one
SELECT COUNT(*) FROM organizations WHERE organization_type = 1;

-- name: CountSchools :one
SELECT COUNT(*) FROM organizations WHERE organization_type = 2;

-- name: CountOrganizationIndustries :one
SELECT COUNT(*) FROM organization_industries;

-- name: CountOrganizationSpecialties :one
SELECT COUNT(*) FROM organization_specialties;

-- name: CountOrganizationLocations :one
SELECT COUNT(*) FROM organization_locations;