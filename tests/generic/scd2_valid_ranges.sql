{#-
  Model test for SCD Type 2 tables. For each entity, checks that:
    1. every version ends after it starts (valid_from < valid_to)
    2. no two versions overlap
    3. versions are contiguous: each ends exactly when the next starts
       (set check_gaps: false for timelines that may have gaps)
    4. at most one version is open-ended (valid_to is null), and it is the last
    5. if current_flag is given: it is true exactly when valid_to is null
    6. if deleted_at is given: a deleted entity's last version ends at deleted_at

  Returns one row per problem, with a description.

  Usage:
    data_tests:
      - scd2_valid_ranges:
          arguments:
            entity_key: customer_id
            current_flag: is_current
-#}
{% test scd2_valid_ranges(
    model,
    entity_key,
    valid_from='valid_from_timestamp',
    valid_to='valid_to_timestamp',
    current_flag=none,
    deleted_at=none,
    check_gaps=true
) %}

with versions as (

    select
        {{ entity_key }}  as entity_key,
        {{ valid_from }}  as valid_from,
        {{ valid_to }}    as valid_to,
        {{ current_flag if current_flag else 'null' }} as current_flag,
        {{ deleted_at if deleted_at else 'null' }}     as deleted_at,
        lead({{ valid_from }}) over (
            partition by {{ entity_key }} order by {{ valid_from }}
        ) as next_valid_from
    from {{ model }}

),

problems as (

    select entity_key, valid_from, valid_to, 'version ends before it starts' as problem
    from versions
    where valid_to <= valid_from

    union all

    select entity_key, valid_from, valid_to, 'overlaps the next version'
    from versions
    where next_valid_from is not null
      and (valid_to is null or valid_to > next_valid_from)

    {% if check_gaps %}
    union all

    select entity_key, valid_from, valid_to, 'gap before the next version'
    from versions
    where next_valid_from is not null
      and valid_to < next_valid_from
    {% endif %}

    union all

    select entity_key, min(valid_from), null, 'more than one open-ended version'
    from versions
    where valid_to is null
    group by entity_key
    having count(*) > 1

    {% if current_flag %}
    union all

    select entity_key, valid_from, valid_to, 'current flag does not match open-ended valid_to'
    from versions
    where (current_flag is distinct from (valid_to is null))
    {% endif %}

    {% if deleted_at %}
    union all

    select entity_key, valid_from, valid_to, 'last version of deleted entity does not end at the delete'
    from versions
    where next_valid_from is null
      and deleted_at is not null
      and (valid_to is distinct from deleted_at)
    {% endif %}

)

select * from problems

{% endtest %}
