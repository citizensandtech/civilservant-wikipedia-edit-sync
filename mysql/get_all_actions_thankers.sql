-- ------
-- Getting all the actions of the thankers and their timestamps.
-- ------
with thankers as (select json_unquote(metadata_json->'$.sync_object.lang') as lang,
                         json_unquote(metadata_json->'$.sync_object.user_name') as user_name
                  from core_experiment_things
                      where experiment_id=-1
                      and randomization_condition='main'),
     thankers_o as (select user_name, lang, cou.id
                    from core_oauth_users cou
                    right join thankers
                      on concat(thankers.lang,':', thankers.user_name) = cou.username),
     thankers_actions as (select user_name, lang, to2.id as oauth_id, created_dt, action
                          from core_experiment_actions cea
                               right join thankers_o to2
                               on cea.action_key_id=to2.id
                                )
select * from thankers_actions;
