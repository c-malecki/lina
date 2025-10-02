-- name: InsertNewConnectionsFromTmp :exec
INSERT INTO connections
(network_id, person_id)
SELECT
  t.network_id,
  t.person_id
FROM tmp_connections t
LEFT JOIN connections c ON c.person_id = t.person_id
WHERE t.person_id IS NOT NULL
  AND c.id IS NULL;

-- name: DeleteConnectionsNotInTmp :exec
DELETE FROM connections
WHERE network_id = ?
AND NOT EXISTS (
  SELECT 1 
  FROM tmp_connections t 
  WHERE t.person_id = connections.person_id
);