WITH combined_raw_data AS
(
       SELECT*
       EXCEPT
              (start_station_id, end_station_id)
       FROM   `cyclistic-344922.cyclistic_td.202103_td`
       UNION ALL
       SELECT*
       EXCEPT
              (start_station_id, end_station_id)
       FROM   `cyclistic-344922.cyclistic_td.202104_td`
       UNION ALL
       SELECT*
       EXCEPT
              (start_station_id, end_station_id)
       FROM   `cyclistic-344922.cyclistic_td.202105_td`
       UNION ALL
       SELECT*
       EXCEPT
              (start_station_id, end_station_id)
       FROM   `cyclistic-344922.cyclistic_td.202106_td`
       UNION ALL
       SELECT*
       EXCEPT
              (start_station_id, end_station_id)
       FROM   `cyclistic-344922.cyclistic_td.202107_td`
       UNION ALL
       SELECT*
       EXCEPT
              (start_station_id, end_station_id)
       FROM   `cyclistic-344922.cyclistic_td.202108_td`
       UNION ALL
       SELECT*
       EXCEPT
              (start_station_id, end_station_id)
       FROM   `cyclistic-344922.cyclistic_td.202109_td`
       UNION ALL
       SELECT*
       EXCEPT
              (start_station_id, end_station_id)
       FROM   `cyclistic-344922.cyclistic_td.202110_td`
       UNION ALL
       SELECT*
       EXCEPT
              (start_station_id, end_station_id)
       FROM   `cyclistic-344922.cyclistic_td.202111_td`
       UNION ALL
       SELECT*
       EXCEPT
              (start_station_id, end_station_id)
       FROM   `cyclistic-344922.cyclistic_td.202112_td`
       UNION ALL
       SELECT*
       EXCEPT
              (start_station_id, end_station_id)
       FROM   `cyclistic-344922.cyclistic_td.202201_td`
       UNION ALL
       SELECT*
       EXCEPT
              (start_station_id, end_station_id)
       FROM   `cyclistic-344922.cyclistic_td.202202_td` ), remove_nulls AS
(
       SELECT*
       FROM   combined_raw_data
       WHERE  ride_id IS NOT NULL
       AND    rideable_type IS NOT NULL
       AND    started_at IS NOT NULL
       AND    ended_at IS NOT NULL
       AND    start_station_name IS NOT NULL
       AND    end_station_name IS NOT NULL
       AND    start_lat IS NOT NULL
       AND    start_lng IS NOT NULL
       AND    end_lat IS NOT NULL
       AND    end_lng IS NOT NULL
       AND    member_casual IS NOT NULL ), ride_day_of_week AS
(
       SELECT *,
              timestamp_diff(ended_at, started_at, minute) AS ride_length_minute,
              CASE
                     WHEN extract(dayofweek FROM started_at) = 1 THEN 'Sunday'
                     WHEN extract(dayofweek FROM started_at) = 2 THEN 'Monday'
                     WHEN extract(dayofweek FROM started_at) = 3 THEN 'Tuesday'
                     WHEN extract(dayofweek FROM started_at) = 4 THEN 'Wednesday'
                     WHEN extract(dayofweek FROM started_at) = 5 THEN 'Thursday'
                     WHEN extract(dayofweek FROM started_at) = 6 THEN 'Friday'
                     ELSE 'Saturday'
              END AS day_of_week
       FROM   remove_nulls ), ride_month AS
(
       SELECT *,
              CASE
                     WHEN extract(month FROM started_at) = 1 THEN 'January'
                     WHEN extract(month FROM started_at) = 2 THEN 'February'
                     WHEN extract(month FROM started_at) = 3 THEN 'March'
                     WHEN extract(month FROM started_at) = 4 THEN 'April'
                     WHEN extract(month FROM started_at) = 5 THEN 'May'
                     WHEN extract(month FROM started_at) = 6 THEN 'June'
                     WHEN extract(month FROM started_at) = 7 THEN 'July'
                     WHEN extract(month FROM started_at) = 8 THEN 'August'
                     WHEN extract(month FROM started_at) = 9 THEN 'September'
                     WHEN extract(month FROM started_at) = 10 THEN 'October'
                     WHEN extract(month FROM started_at) = 11 THEN 'November'
                     ELSE 'December'
              END AS ride_month
       FROM   ride_day_of_week ), ride_start_hour AS
(
       SELECT *,
              extract (hour FROM started_at) AS start_hour
       FROM   ride_month ), cleaned_data AS
(
       SELECT*
       FROM   ride_start_hour
       WHERE  length(ride_id) = 16
       AND    ride_length_minute >= 1 ), clean_coord AS
(
       SELECT ride_id,
              round(start_lat,5) AS start_lat,
              round(end_lat,5)   AS end_lat,
              round(start_lng,5) AS start_long,
              round(end_lng,5)   AS end_long,
       FROM   cleaned_data ), lat_lng AS
(
       SELECT ride_id,
                     concat(start_lat, ', ', start_long) AS starting_lat_long,
                     concat(end_lat, ', ', end_long)     AS ending_lat_long
       FROM   clean_coord ), join_td AS
(
       SELECT cd.ride_id            AS trip_id,
              cd.rideable_type      AS bike_type,
              cd.member_casual      AS user_type,
              cd.ride_length_minute AS length_of_ride,
              cd.ride_month         AS month,
              cd.day_of_week,
              cd.start_hour AS ride_time,
              cd.start_station_name,
              ll.starting_lat_long,
              cd.end_station_name,
              ll.ending_lat_long
       FROM   cleaned_data AS cd
       JOIN   lat_lng      AS ll
       ON     cd.ride_id = ll.ride_id )
SELECT*
FROM   join_td
