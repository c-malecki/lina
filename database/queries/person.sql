-- name: InsertPerson :exec
INSERT INTO persons
(first_name, last_name, headline, profile_url, public_identifier, profile_picture_url, about, location_id, urn, created_at)
VALUES
(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);