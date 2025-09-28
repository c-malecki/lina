-- name: InsertExperience :exec
INSERT INTO experiences (person_id, organization_id, title, location_raw, "description", start_year, start_month, is_current, end_year, end_month, skills_url)
VALUES
(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);