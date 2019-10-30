-- edit counts of unremoved uncompleted users
set @whatever_reason_not_enough_edits_removed_dt = date(20191030);
with uncompleted_unremoved_thankees as
(select
   created_at,
   lang,
   user_name,
   user_id,
   user_experience_level,
   randomization_arm,
   concat('user:',
          json_unquote(metadata_json->'$.sync_object.lang'),
          ':',
          json_unquote(metadata_json->'$.sync_object.user_id')) as query_index
 from core_experiment_things cet
   join core_candidates cc
     on cet.metadata_json->'$.sync_object.lang' = cc.lang
        and cet.metadata_json->'$.sync_object.user_name' = cc.user_name
 where cet.experiment_id = -3
       and cet.removed_dt is NULL
       and cet.randomization_arm = 1
       and cc.user_completed=0
       and cc.lang !='en'),

    uncompleted_unremoved_edits as
  (select
     lang,
     user_name,
     cet2.query_index,
     user_experience_level,
     cet2.id as edit_id,
     case when removed_dt is null then 1 else 0 end as unremoved_edit
   from uncompleted_unremoved_thankees
     left join core_experiment_things cet2
       on uncompleted_unremoved_thankees.query_index = cet2.query_index),

    not_enough_edits_users as (select sum(unremoved_edit), lang, user_name
        from uncompleted_unremoved_edits
        group by lang, user_name
        having sum(unremoved_edit)<4)

update  core_experiment_things cet3
  join not_enough_edits_users neeu
on cet3.metadata_json->'$.sync_object.user_name' = neeu.user_name
and cet3.metadata_json->'$.sync_object.lang' = neeu.lang
set removed_dt=@whatever_reason_not_enough_edits_removed_dt;
