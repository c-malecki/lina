-- name: InsertNewConnectionsFromTmp :exec
INSERT INTO connections
(network_id, person_id)
SELECT
  t.network_id,
  t.person_id
FROM tmp_connections t
LEFT JOIN connections c ON c.person_id = t.person_id
  AND c.network_id = ?
WHERE t.person_id IS NOT NULL
  AND c.id IS NULL;