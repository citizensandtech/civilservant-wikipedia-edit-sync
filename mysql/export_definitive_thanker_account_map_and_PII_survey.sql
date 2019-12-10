-- test that all of these users have a non-null anonymized id
-- select count(*), metadata_json->'$.sync_object.anonymized_id' is NULL
-- from core_experiment_things
--   where experiment_id=-1 and randomization_condition='main'
-- group by metadata_json->'$.sync_object.anonymized_id' is NULL;
--

-- first the account map
 select json_unquote(metadata_json->'$.sync_object.anonymized_id') as anonymized_id,
        json_unquote(metadata_json->'$.sync_object.lang') as lang,
        json_unquote(metadata_json->'$.sync_object.user_name') as user_name
from core_experiment_things
  where experiment_id=-1
        and randomization_condition='main'
        and json_unquote(metadata_json->'$.sync_object.lang')!='en';

-- second the PII and sensitive data the account map
select
  json_unquote(metadata_json->'$.sync_object.anonymized_id') as anonymized_id,
  json_unquote(metadata_json->'$.sync_object.num_reverts_84_pre_treatment') as num_reverts_84_pre_treatment,
  json_unquote(metadata_json->'$.sync_object.project_talk_84_pre_treatment') as project_talk_84_pre_treatment,
  json_unquote(metadata_json->'$.sync_object.support_talk_84_pre_treatment') as support_talk_84_pre_treatment,
  json_unquote(metadata_json->'$.sync_object.is_official_bot') as is_official_bot,
  json_unquote(metadata_json->'$.sync_object.wikithank_84_pre_treatment') as wikithank_84_pre_treatment,
  json_unquote(metadata_json->'$.sync_object.wikilove_84_pre_treatment') as wikilove_84_pre_treatment,
  json_unquote(metadata_json->'$.sync_object.block_actions_84_pre_treatment') as block_actions_84_pre_treatment,
  json_unquote(metadata_json->'$.sync_object.has_email') as has_email,
  json_unquote(metadata_json->'$.sync_object.pre_monitoring_damaging_content') as pre_monitoring_damaging_content,
  json_unquote(metadata_json->'$.sync_object.pre_mentoring') as pre_mentoring,
  json_unquote(metadata_json->'$.sync_object.pre_newcomer_capability') as pre_newcomer_capability,
  json_unquote(metadata_json->'$.sync_object.pre_newcomer_intent') as pre_newcomer_intent,
  json_unquote(metadata_json->'$.sync_object.pre_emotionally_draining') as pre_emotionally_draining,
  json_unquote(metadata_json->'$.sync_object.pre_feel_positive') as pre_feel_positive,
  json_unquote(metadata_json->'$.sync_object.supportive_84_pre_treatment') as supportive_84_pre_treatment,
  json_unquote(metadata_json->'$.sync_object.randomization_block_id') as randomization_block_id,
  json_unquote(metadata_json->'$.sync_object.randomization_block_size') as randomization_block_size,
  randomization_arm
from core_experiment_things
  where experiment_id=-1
        and randomization_condition='main'
        and json_unquote(metadata_json->'$.sync_object.lang')!='en';
