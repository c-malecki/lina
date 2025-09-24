-- name: SelectConfig :many
SELECT * FROM config LIMIT 1;

-- name: UpdateConfigSecret :exec
UPDATE config SET "secret" = ? WHERE id = ?;