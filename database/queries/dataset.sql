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

-- name: SelectDatasetDegreesByName :many
SELECT * FROM dataset_degrees WHERE "name" IN (sqlic.slice(names));

-- name: SelectDatasetIndustryByName :many
SELECT * FROM dataset_industries WHERE "name" IN (sqlic.slice(names));

-- name: SelectDatasetLocationByName :many
SELECT * FROM dataset_locations WHERE "name" IN (sqlic.slice(names));

-- name: SelectDatasetSkillByName :many
SELECT * FROM dataset_skills WHERE "name" IN (sqlic.slice(names));

-- name: SelectDatasetSpecialtyByName :many
SELECT * FROM dataset_specialties WHERE "name" IN (sqlic.slice(names));

-- name: SelectDatasetStudyFieldByName :many
SELECT * FROM dataset_study_fields WHERE "name" IN (sqlic.slice(names));