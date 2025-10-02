-- name: UpdateTmpConnectionPersonIDs :exec
UPDATE tmp_connections
SET person_id = (
  SELECT p.id 
  FROM persons p 
  WHERE p.profile_url = tmp_connections.profile_url
)
WHERE EXISTS (
  SELECT 1 
  FROM persons p 
  WHERE p.profile_url = tmp_connections.profile_url
);

-- name: SelectTmpConnectionsNoPersonIDs :many
SELECT profile_url FROM tmp_connections WHERE person_id IS NULL;

-- name: CountConnectionsToAdd :one
SELECT COUNT(t.id)
FROM tmp_connections t
LEFT JOIN connections c ON c.person_id = t.person_id
  AND c.network_id = ?
WHERE c.id IS NULL AND t.person_id IS NOT NULL;

-- name: CountConnectionsToRemove :one
SELECT COUNT(c.id)
FROM connections c
LEFT JOIN tmp_connections t ON t.person_id = c.person_id
  AND t.person_id IS NOT NULL
WHERE t.id IS NULL AND c.network_id = ?;