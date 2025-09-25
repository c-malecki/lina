-- name: SelectNetworkByUserID :one
SELECT * FROM networks WHERE user_id = ?;

-- name: SelectNetworks :many
SELECT * FROM networks WHERE user_id = ? LIMIT ? OFFSET ?;

-- name: InsertNetwork :exec
INSERT INTO networks (user_id, "name") VALUES (?, ?);

-- name: SelectPersonsByLinkedinURLs :many
SELECT id, profile_url FROM persons WHERE profile_url IN (sqlc.slice(linkedin_urls));

-- name: SelectPersonsByNetworkConnections :many
SELECT p.id, p.profile_url
FROM network_connections nc
INNER JOIN persons p ON p.id = nc.person_id
WHERE network_id = ?;