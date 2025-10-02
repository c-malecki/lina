-- name: InsertEducation :exec
INSERT INTO educations
(person_id, organization_id, degree_id, study_field_id, start_year, start_month, end_year, end_month)
VALUES
(?, ?, ?, ?, ?, ?, ?, ?);

-- name: CountEducations :one
SELECT COUNT(*) FROM educations;