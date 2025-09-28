-- name: InsertDatasetDegree :exec
INSERT INTO dataset_degrees (id, "name") VALUES (?, ?);

-- name: InsertDatasetIndustry :exec
INSERT INTO dataset_industries (id, "name") VALUES (?, ?);

-- name: InsertDatasetLocation :exec
INSERT INTO dataset_locations
(id, "name", city, "state", country, country_code)
VALUES
(?, ?, ?, ?, ?, ?);

-- name: InsertDatasetSkill :exec
INSERT INTO dataset_skills (id, "name") VALUES (?, ?);

-- name: InsertDatasetSpecialty :exec
INSERT INTO dataset_specialties (id, "name") VALUES (?, ?);

-- name: InsertDatasetStudyField :exec
INSERT INTO dataset_study_fields (id, "name") VALUES (?, ?);