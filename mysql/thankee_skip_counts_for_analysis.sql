with thankees as (select cet.metadata_json->'$.sync_object.lang' as lang,
                    cet.metadata_json->'$.sync_object.user_name' as user_name,
                    date(cet.created_dt) as onboard_date,
                    cc.user_completed as user_completed,
                    cc.user_experience_level as user_experience_level
                  from core_experiment_things cet
                    join core_candidates cc
                      on cet.metadata_json->'$.sync_object.lang'=cc.lang
                        and cet.metadata_json->'$.sync_object.user_name'=cc.user_name
                  where experiment_id=-3
                        and randomization_arm=1
                        and removed_dt is NULL),
    skip_counts as (select metadata_json->'$.lang' as lang,
                      action_object_id as user_name,
                      count(*)         as num_skips
                    from core_experiment_actions
                    where action='skip' group by action_object_id, metadata_json->'$.lang')
select thankees.lang,
  thankees.user_name,
  thankees.user_completed,
  thankees.user_experience_level,
  thankees.onboard_date,
  skip_counts.num_skips
from thankees
left join skip_counts
on thankees.lang = skip_counts.lang and thankees.user_name=skip_counts.user_name;
