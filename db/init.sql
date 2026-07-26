CREATE TABLE events (
    position bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    type TEXT NOT NULL,
    data jsonb,
    id uuid NOT NULL UNIQUE
);

CREATE TABLE streams (
    name text NOT NULL,
    event_id uuid NOT NULL REFERENCES events (id),
    position bigint NOT NULL,
    UNIQUE (name, position),
    UNIQUE (event_id, name)
);

CREATE TABLE tags (
    key TEXT NOT NULL,
    value text NOT NULL,
    event_id uuid NOT NULL REFERENCES events (id),
    UNIQUE (event_id, key, value)
);

-- Hash the value column so adjacent tag values scatter across distinct
-- btree leaf pages. Under SSI this dramatically reduces SIREAD-vs-INSERT
-- page collisions among concurrent writers whose tag values cluster
-- lexicographically (e.g. "bench_<run>_0..9"). Equality queries on tag
-- value must include the same hashtext(value) predicate to use this index.
CREATE INDEX ON tags (key, hashtext(value), event_id);

CREATE FUNCTION read_stream (p_stream_name text)
    RETURNS SETOF events
    LANGUAGE sql
    STABLE
    AS $$
    SELECT
        e.*
    FROM
        events e
        JOIN streams s ON s.event_id = e.id
    WHERE
        s.name = p_stream_name
    ORDER BY
        s.position;
$$;

CREATE FUNCTION read_tags (p_tags jsonb, p_types text[])
    RETURNS SETOF events
    LANGUAGE sql
    STABLE
    AS $$
    WITH matching AS (
        SELECT
            t.event_id
        FROM
            tags t
            JOIN jsonb_each_text(p_tags) AS pairs (k,
                v) ON t.key = pairs.k
                AND hashtext(t.value) = hashtext(pairs.v)
                AND t.value = pairs.v
        GROUP BY
            t.event_id
        HAVING
            count(*) = (
                SELECT
                    count(*)
                FROM
                    jsonb_each_text(p_tags)))
    SELECT
        e.*
    FROM
        matching m
        JOIN events e ON e.id = m.event_id
    WHERE
        e.type = ANY (p_types)
    ORDER BY
        e.position;
$$;

CREATE FUNCTION append_events (p_events jsonb, p_stream_name text, p_expected_position bigint)
    RETURNS void
    LANGUAGE sql
    AS $$
    WITH ordered_input AS MATERIALIZED (
        SELECT
            elem ->> 'type' AS type,
            elem -> 'data' AS data,
            uuidv7 (
) AS id,
            ord
        FROM
            jsonb_array_elements(
                p_events
) WITH ORDINALITY AS arr ( elem, ord
)
),
inserted_events AS (INSERT INTO events (type, data, id)
SELECT
    type,
    data,
    id
FROM
    ordered_input)
INSERT INTO streams (name, event_id, position)
SELECT
    p_stream_name,
    id,
    p_expected_position + ord
FROM
    ordered_input;
$$;

CREATE INDEX events_type_idx ON events (type);

CREATE FUNCTION append_events (p_events jsonb, p_append_condition jsonb DEFAULT '{}'::jsonb)
    RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE
    criteria jsonb[] := ARRAY (
        SELECT
            jsonb_array_elements(COALESCE(p_append_condition -> 'fail_if_events_match', '[]'::jsonb)));
BEGIN
    IF cardinality(criteria) > 0 AND EXISTS (
        SELECT
            1
        FROM
            unnest(criteria) AS c
        WHERE EXISTS (
            SELECT
                1
            FROM (
                SELECT
                    t.event_id
                FROM
                    tags t
                    JOIN jsonb_each_text(COALESCE(c -> 'tags', '{}'::jsonb)) AS req (k, v)
                        ON t.key = req.k AND hashtext(t.value) = hashtext(req.v) AND t.value = req.v
                GROUP BY
                    t.event_id
                HAVING
                    count(*) = (SELECT count(*) FROM jsonb_each_text(COALESCE(c -> 'tags', '{}'::jsonb)))
            ) AS cand
            JOIN events e ON e.id = cand.event_id
            WHERE ((c ->> 'after')::bigint IS NULL OR e.position > (c ->> 'after')::bigint)
                AND (c -> 'types' IS NULL OR e.type IN (
                    SELECT jsonb_array_elements_text(c -> 'types')))
        )) THEN
        RAISE EXCEPTION 'append_condition_violated';
    END IF;
    WITH ordered_input AS MATERIALIZED (
        SELECT
            elem ->> 'type' AS type,
            elem -> 'data' AS data,
            elem -> 'tags' AS tags,
            gen_random_uuid () AS id
        FROM
            jsonb_array_elements(p_events) AS arr (elem)
    ),
    inserted_events AS (
        INSERT INTO events (type, data, id)
        SELECT type, data, id FROM ordered_input
    )
    INSERT INTO tags (key, value, event_id)
    SELECT pair.k, pair.v, oi.id
    FROM ordered_input oi
        CROSS JOIN LATERAL jsonb_each_text(COALESCE(oi.tags, '{}'::jsonb)) AS pair (k, v);
END;
$$;

CREATE FUNCTION append_events_locked (p_events jsonb, p_append_condition jsonb DEFAULT '{}'::jsonb)
    RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE
    criteria jsonb[] := ARRAY (
        SELECT
            jsonb_array_elements(COALESCE(p_append_condition -> 'fail_if_events_match', '[]'::jsonb)));
    lock_keys text[];
    lock_key text;
BEGIN
    IF cardinality(criteria) > 0 THEN
        -- Sort criterion serialisations alphabetically to acquire locks in a
        -- deterministic order across concurrent writers (deadlock prevention).
        lock_keys := ARRAY (
            SELECT DISTINCT c::text FROM unnest(criteria) AS c ORDER BY 1
        );

        FOREACH lock_key IN ARRAY lock_keys LOOP
            PERFORM pg_advisory_xact_lock(hashtext(lock_key));
        END LOOP;
    END IF;

    IF cardinality(criteria) > 0 AND EXISTS (
        SELECT
            1
        FROM
            unnest(criteria) AS c
        WHERE EXISTS (
            SELECT
                1
            FROM (
                SELECT
                    t.event_id
                FROM
                    tags t
                    JOIN jsonb_each_text(COALESCE(c -> 'tags', '{}'::jsonb)) AS req (k, v)
                        ON t.key = req.k AND hashtext(t.value) = hashtext(req.v) AND t.value = req.v
                GROUP BY
                    t.event_id
                HAVING
                    count(*) = (SELECT count(*) FROM jsonb_each_text(COALESCE(c -> 'tags', '{}'::jsonb)))
            ) AS cand
            JOIN events e ON e.id = cand.event_id
            WHERE ((c ->> 'after')::bigint IS NULL OR e.position > (c ->> 'after')::bigint)
                AND (c -> 'types' IS NULL OR e.type IN (
                    SELECT jsonb_array_elements_text(c -> 'types')))
        )) THEN
        RAISE EXCEPTION 'append_condition_violated';
    END IF;

    WITH ordered_input AS MATERIALIZED (
        SELECT
            elem ->> 'type' AS type,
            elem -> 'data' AS data,
            elem -> 'tags' AS tags,
            uuidv7 () AS id
        FROM
            jsonb_array_elements(p_events) AS arr (elem)
    ),
    inserted_events AS (
        INSERT INTO events (type, data, id)
        SELECT type, data, id FROM ordered_input
    )
    INSERT INTO tags (key, value, event_id)
    SELECT pair.k, pair.v, oi.id
    FROM ordered_input oi
    CROSS JOIN LATERAL jsonb_each_text(COALESCE(oi.tags, '{}'::jsonb)) AS pair (k, v);
END;
$$;

INSERT INTO events (type, data, id)
SELECT
    'type_' || (1 + (i - 1) % 10),
    jsonb_build_object('key_1', md5(random()::text), 'key_2', md5(random()::text), 'key_3', md5(random()::text), 'key_4', md5(random()::text), 'key_5', md5(random()::text), 'key_6', md5(random()::text), 'key_7', md5(random()::text), 'key_8', md5(random()::text), 'key_9', md5(random()::text), 'key_10', md5(random()::text)),
    uuidv7 ()
FROM
    generate_series(1, 1000000) AS s (i);

INSERT INTO streams (name, event_id, position)
SELECT
    'stream_100_000',
    id,
    position / 10
FROM
    events
WHERE
    position % 10 = 0;

INSERT INTO streams (name, event_id, position)
SELECT
    'stream_10_000',
    id,
    position / 100
FROM
    events
WHERE
    position % 100 = 0;

INSERT INTO streams (name, event_id, position)
SELECT
    'stream_1_000',
    id,
    position / 1000
FROM
    events
WHERE
    position % 1000 = 0;

INSERT INTO streams (name, event_id, position)
SELECT
    'stream_100',
    id,
    position / 10000
FROM
    events
WHERE
    position % 10000 = 0;

INSERT INTO streams (name, event_id, position)
SELECT
    'stream_10',
    id,
    position / 100000
FROM
    events
WHERE
    position % 100000 = 0;

INSERT INTO streams (name, event_id, position)
SELECT
    'stream_1',
    id,
    position / 1000000
FROM
    events
WHERE
    position % 1000000 = 0;

INSERT INTO tags (key, value, event_id)
SELECT
    'name',
    'stream_100_000',
    id
FROM
    events
WHERE
    position % 10 = 0;

INSERT INTO tags (key, value, event_id)
SELECT
    'name',
    'stream_10_000',
    id
FROM
    events
WHERE
    position % 100 = 0;

INSERT INTO tags (key, value, event_id)
SELECT
    'name',
    'stream_1_000',
    id
FROM
    events
WHERE
    position % 1000 = 0;

INSERT INTO tags (key, value, event_id)
SELECT
    'name',
    'stream_100',
    id
FROM
    events
WHERE
    position % 10000 = 0;

INSERT INTO tags (key, value, event_id)
SELECT
    'name',
    'stream_10',
    id
FROM
    events
WHERE
    position % 100000 = 0;

INSERT INTO tags (key, value, event_id)
SELECT
    'name',
    'stream_1',
    id
FROM
    events
WHERE
    position % 1000000 = 0;

