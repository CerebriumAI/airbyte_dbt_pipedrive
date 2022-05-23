select distinct on (id)
        id as activity_id,
        date_trunc('day', date(update_time)) as updated_at_date,
        type,
        done as is_done,
        assigned_to_user_id
    from
        {{ var('activities') }}

