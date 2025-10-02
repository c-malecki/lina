-- name: InsertDatasetDegree :exec
INSERT INTO dataset_degrees ("name") VALUES (?);

-- name: InsertDatasetIndustry :exec
INSERT INTO dataset_industries ("name") VALUES (?);

-- name: InsertDatasetLocation :exec
INSERT INTO dataset_locations
("name", city, "state", country)
VALUES
(?, ?, ?, ?);

-- name: InsertDatasetSkill :exec
INSERT INTO dataset_skills ("name") VALUES (?);

-- name: InsertDatasetSpecialty :exec
INSERT INTO dataset_specialties ("name") VALUES (?);

-- name: InsertDatasetStudyField :exec
INSERT INTO dataset_study_fields ("name") VALUES (?);

-- name: CountDatasetDegrees :one
SELECT COUNT(*) FROM dataset_degrees;

-- name: CountDatasetIndustries :one
SELECT COUNT(*) FROM dataset_industries;

-- name: CountDatasetLocations :one
SELECT COUNT(*) FROM dataset_locations;

-- name: CountDatasetSkills :one
SELECT COUNT(*) FROM dataset_skills;

-- name: CountDatasetSpecialties :one
SELECT COUNT(*) FROM dataset_specialties;

-- name: CountDatasetStudyFields :one
SELECT COUNT(*) FROM dataset_study_fields;