--
-- :create_table
CREATE TABLE IF NOT EXISTS erleans_grains (
    grain_id BYTEA NOT NULL,
    grain_type CHARACTER VARYING(2048),
    grain_ref_hash BIGINT NOT NULL,
    grain_etag BIGINT NOT NULL,
    grain_state BYTEA NOT NULL,
    change_time TIMESTAMP NOT NULL,
    PRIMARY KEY (grain_id, grain_type))

--
-- :create_idx
CREATE INDEX IF NOT EXISTS
    erleans_grains_term_idx
    ON erleans_grains
    USING HASH (grain_ref_hash)

--
-- :select_all
SELECT
    grain_id, grain_etag, grain_state
FROM
    erleans_grains
WHERE
    grain_type = $1
    AND grain_type = $2

--
-- :select
SELECT
    grain_id, grain_type, grain_etag, grain_state
FROM
    erleans_grains
WHERE grain_ref_hash = $1
    AND grain_type = $2

--
-- :insert
INSERT INTO
    erleans_grains (
        grain_id,
        grain_type,
        grain_ref_hash,
        grain_etag,
        grain_state,
        change_time )
    VALUES
       ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP)

--
-- :update
UPDATE
    erleans_grains
SET
    grain_etag = $1,
    grain_state = $2,
    change_time = CURRENT_TIMESTAMP
WHERE
    grain_ref_hash = $3 AND grain_id = $4 AND grain_type = $5 AND grain_etag = $6

--
-- :delete
DELETE FROM
    erleans_grains
WHERE
    grain_ref_hash = $1 AND grain_id = $2 AND grain_type = $3
