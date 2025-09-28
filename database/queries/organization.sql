-- name: InsertOrganization :exec
INSERT INTO organizations
(id, "name", universal_name, website, linkedin_url, logo_url, founded_year, founded_month, organization_type, employee_count, student_count, urn, created_at)
VALUES
(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

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