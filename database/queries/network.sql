-- name: SelectNetworkByUserID :one
SELECT * FROM networks WHERE user_id = ?;

-- name: SelectNetworks :many
SELECT * FROM networks WHERE user_id = ? LIMIT ? OFFSET ?;

-- name: InsertNetwork :exec
INSERT INTO networks (user_id, "name") VALUES (?, ?);

-- name: SelectPersonsByConnections :many
SELECT p.id, p.profile_url
FROM connections nc
INNER JOIN persons p ON p.id = nc.person_id
WHERE network_id = ?;

-- name: InsertConnection :exec
INSERT INTO connections
(network_id, person_id)
VALUES
(?, ?);