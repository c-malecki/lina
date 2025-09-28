-- name: SelectPersonsByLinkedinURLs :many
SELECT id, profile_url FROM persons WHERE profile_url IN (sqlc.slice(linkedin_urls));

-- name: InsertPerson :exec
INSERT INTO persons
(id, first_name, last_name, headline, profile_url, public_identifier, profile_picture_url, about, location_id, urn, current_company_id, created_at)
VALUES
(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);