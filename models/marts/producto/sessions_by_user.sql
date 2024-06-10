WITH count_events_by_user_and_session AS (
    SELECT * FROM {{ref('int_count_events_by_user_and_session')}}
    ),

    sessions_by_user AS(
        SELECT * FROM {{ref('int_data_sessions_users')}}
    ),

    joined AS (
        SELECT  a.user_id
                ,a.session_id
                ,a.first_event_time_utc
                ,a.last_event_time_utc
                ,a.checkout_event_time_utc
                --,a.package_shipped_event_time_utc
                ,a.sessions_total_length_min
                ,a.sessions_init_to_checkout_length_min
                ,a.preparing_to_shipped_length_min
                --,a.preparing_to_shipped_length_hour
                --,a.preparing_to_shipped_length_day
                ,b.checkout_amount
                ,b.package_shipped_amount
                ,b.add_to_cart_amount
                ,b.page_view_amount               
                FROM sessions_by_user a
                JOIN count_events_by_user_and_session b
                ON a.user_id = b.user_id and a.session_id = b.session_id
    )
    SELECT * FROM joined order by user_id

