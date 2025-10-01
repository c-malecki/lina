-- name: CreateTmpConnectionsTable :exec
CREATE TEMPORARY TABLE IF NOT EXISTS tmp_connections (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  network_id INTEGER NOT NULL,
  profile_url TEXT NOT NULL,
  person_id INTEGER
);

-- name: InsertTmpConnection :exec
INSERT INTO tmp_connections (network_id, profile_url) VALUES (?, ?);