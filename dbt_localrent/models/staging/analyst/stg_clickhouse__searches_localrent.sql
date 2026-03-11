{{ config(
    order_by='(`timestamp`, search_id, search_uuid, session_id)',
    engine='MergeTree()',
	partition_by='toYYYYMM(timestamp)',
    materialized='incremental',
    incremental_strategy='append')
}}

WITH date_range AS (
  SELECT
    {% if var('chunk_start', false) and var('chunk_end', false) %}
      toDateTime({{ var('chunk_start') }}) AS start_dt,
      toDateTime({{ var('chunk_end')   }}) AS end_dt
    {% elif is_incremental() %}
      addDays(now(), -7)                   AS start_dt,
      now()                                AS end_dt
    {% else %}
      toDateTime('2026-03-01')             AS start_dt,
      now()                                AS end_dt
    {% endif %}
)

SELECT *
FROM (
  SELECT
      ss.`timestamp`                                        AS `timestamp`,
      assumeNotNull(ss.search_id)                           AS search_id,
      assumeNotNull(ss.session_id)                          AS session_id,
      ss.search_uuid                                        AS search_uuid,
      ss.pickup_place_id                                    AS pickup_place_id,
      ss.pickup_city_id                                     AS pickup_city_id,
      ss.country_id                                         AS country_id,
      ss.aff_id                                             AS aff_id,
      ss.referer                                             AS referer,
      ss.referer_host                                        AS referer_host,
      ss.referer_path                                        AS referer_path,

      /* Dates extracted from params JSON */
      toDate(JSONExtractString(ss.params, 'pickup_date'))   AS pickup_date,
      toDate(JSONExtractString(ss.params, 'dropoff_date'))  AS dropoff_date,

      /* Derived durations (avoid alias dependency by recomputing expressions) */
      toDate(JSONExtractString(ss.params, 'dropoff_date'))
        - toDate(JSONExtractString(ss.params, 'pickup_date'))        AS rent_duration,
      toDate(JSONExtractString(ss.params, 'pickup_date'))
        - toDate(ss.`timestamp`)                                     AS rent_offset,

      /* Heuristic flag */
      IF (
        (
          (toDate(JSONExtractString(ss.params, 'dropoff_date'))
           - toDate(JSONExtractString(ss.params, 'pickup_date'))) = 14
          AND
          (toDate(JSONExtractString(ss.params, 'pickup_date'))
           - toDate(ss.`timestamp`)) = 3
        )
        OR
        (
          (toDate(JSONExtractString(ss.params, 'dropoff_date'))
           - toDate(JSONExtractString(ss.params, 'pickup_date'))) = 7
          AND
          (toDate(JSONExtractString(ss.params, 'pickup_date'))
           - toDate(ss.`timestamp`)) = 15
          AND (toDate(ss.`timestamp`) BETWEEN toDate('2024-02-12') AND toDate('2024-05-12'))
        ),
        1, 0
      )                                                       AS is_initial_heu,

      ss.is_initial                                           AS is_initial_param,
      (ss.is_initial OR
       IF (
         (
           (toDate(JSONExtractString(ss.params, 'dropoff_date'))
            - toDate(JSONExtractString(ss.params, 'pickup_date'))) = 14
           AND
           (toDate(JSONExtractString(ss.params, 'pickup_date'))
            - toDate(ss.`timestamp`)) = 3
         )
         OR
         (
           (toDate(JSONExtractString(ss.params, 'dropoff_date'))
            - toDate(JSONExtractString(ss.params, 'pickup_date'))) = 7
           AND
           (toDate(JSONExtractString(ss.params, 'pickup_date'))
            - toDate(ss.`timestamp`)) = 15
           AND (toDate(ss.`timestamp`) BETWEEN toDate('2024-02-12') AND toDate('2024-05-12'))
         ),
         1, 0
       )
      )                                                       AS is_initial_combined,

      ss.`source`                                            AS `source`,
      JSONExtractRaw(ss.params, 'ab_flags')                  AS ab_flags,
      ss.user_ip                                             AS user_ip,
      ss.user_agent                                          AS user_agent,
      ss.user_device                                         AS user_device
	FROM
		(
		SELECT DISTINCT ON (search_id, session_id, search_uuid, 'timestamp') *
		FROM {{ source('analyst', 'stat_searches') }} src
		CROSS JOIN date_range dr
		WHERE
			(src.search_uuid IS NOT NULL) AND (src.search_uuid != '')
			AND (src.referer != '' AND src.referer IS NOT NULL)
			AND (src.`timestamp` >= dr.start_dt AND src.`timestamp` < dr.end_dt)

			{% if not (var('chunk_start', false) and var('chunk_end', false)) and is_incremental() %}
			AND (src.`timestamp` >= (SELECT coalesce(max(`timestamp`), toDateTime('1900-01-01')) FROM {{ this }}))
			{% endif %}
	) AS ss
)
