with users as (
    select
        user_id,
        email
    from {{ ref('stg_pipedrive_users') }}
)

select * from users
