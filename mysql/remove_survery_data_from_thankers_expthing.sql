with thankers as (
    select *
    from core_experiment_things
    where experiment_id = -1 and randomization_condition = 'main'
)
update core_experiment_things cet
  join thankers t
    on cet.id = t.id
set cet.metadata_json = JSON_REMOVE(cet.metadata_json,
                                    '$.sync_object.pre_mentoring',
                                    '$.sync_object.pre_feel_positive',
                                    '$.sync_object.pre_newcomer_intent',
                                    '$.sync_object.randomization_block_id',
                                    '$.sync_object.pre_newcomer_capability',
                                    '$.sync_object.pre_emotionally_draining',
                                    '$.sync_object.randomization_block_size',
                                    '$.sync_object.wikilove_84_pre_treatment',
                                    '$.sync_object.wikithank_84_pre_treatment',
                                    '$.sync_object.supportive_84_pre_treatment',
                                    '$.sync_object.num_reverts_84_pre_treatment',
                                    '$.sync_object.project_talk_84_pre_treatment',
                                    '$.sync_object.support_talk_84_pre_treatment',
                                    '$.sync_object.block_actions_84_pre_treatment',
                                    '$.sync_object.pre_monitoring_damaging_content',
                                    '$.sync_object.is_official_bot',
                                    '$.sync_object.anonymized_id');
