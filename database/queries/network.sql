-- name: SelectNetworkByUserID :one
SELECT * FROM networks WHERE user_id = ?;

-- name: SelectNetworks :many
SELECT * FROM networks WHERE user_id = ? LIMIT ? OFFSET ?;

-- name: InsertNetwork :exec
INSERT INTO networks (user_id, "name") VALUES (?, ?);

-- name: SelectPersonsByNetworkConnections :many
SELECT p.id, p.profile_url
FROM network_connections nc
INNER JOIN persons p ON p.id = nc.person_id
WHERE network_id = ?;

-- name: InsertNetworkConnection :exec
INSERT INTO network_connections
(network_id, person_id)
VALUES
(?, ?);