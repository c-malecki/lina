-- name: CountUsers :one
SELECT COUNT(*) FROM users;

-- name: InsertUser :exec
INSERT INTO users (username, "password") VALUES (?, ?);

-- name: SelectUser :one
SELECT * FROM users WHERE username = ?;

-- name: UpdateUserApifyToken :exec
UPDATE users SET apify_token = ? WHERE id = ?;