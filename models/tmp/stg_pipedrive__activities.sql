select distinct on (id)
        id,
        date_trunc('day', date(update_time)) as "date",
        type,
        done,
        assigned_to_user_id
    from
        {{ var('activities') }}

