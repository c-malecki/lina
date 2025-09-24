-- name: CountPersons :one
SELECT COUNT(*) FROM persons;

-- name: CountCompanies :one
SELECT COUNT(*) FROM organizations WHERE organization_type = 0;

-- name: CountSchools :one
SELECT COUNT(*) FROM organizations WHERE organization_type = 1;