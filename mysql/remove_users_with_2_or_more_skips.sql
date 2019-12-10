-- uncompleted users
-- set removed dt
set @skipped_too_often_removed_dt = date(20191115);
set @too_many_skips = 2;
with uncompleted_thankees as (select
                                cet.metadata_json -> '$.sync_object.lang'      as lang,
                                cet.metadata_json -> '$.sync_object.user_name' as user_name,
                                date(cet.created_dt)                           as onboard_date,
                                cc.user_completed                              as user_completed,
                                cc.user_experience_level                       as user_experience_level
                              from core_experiment_things cet
                                join core_candidates cc
                                  on cet.metadata_json -> '$.sync_object.lang' = cc.lang
                                     and cet.metadata_json -> '$.sync_object.user_name' = cc.user_name
                              where experiment_id = -3
                                    and randomization_arm = 1
                                    and removed_dt is NULL
                                    and user_completed is FALSE),
    skip_counts as (select
                      metadata_json -> '$.lang' as lang,
                      action_object_id          as user_name,
                      count(*)                  as num_skips
                    from core_experiment_actions
                    where action = 'skip'
                    group by action_object_id, metadata_json -> '$.lang'),
    uncompleted_skip_counts as (select
                                  uncompleted_thankees.lang,
                                  uncompleted_thankees.user_name,
                                  uncompleted_thankees.user_completed,
                                  uncompleted_thankees.user_experience_level,
                                  uncompleted_thankees.onboard_date,
                                  skip_counts.num_skips
                                from uncompleted_thankees
                                  left join skip_counts
                                    on uncompleted_thankees.lang = skip_counts.lang
                                       and uncompleted_thankees.user_name = skip_counts.user_name),
    too_often_skipped_users as (
                              select *
                              from uncompleted_skip_counts usc
                              where num_skips >= @too_many_skips
                                    and onboard_date > date_sub(date(now()), interval 90 day)
  )
update  core_experiment_things cet3
  join too_often_skipped_users tosu
on cet3.metadata_json->'$.sync_object.user_name' = tosu.user_name
and cet3.metadata_json->'$.sync_object.lang' = tosu.lang
set removed_dt=@skipped_too_often_removed_dt;
