-- علاء الدين already a superthanker
-- Amr F.Nagy        already a superthanker
-- Bachounda         convertible
-- Avicenno          convertible
-- Omar kandil       already a superthanker
-- Mohammad hajeer   already a superthanker

select * from civilservant_production.core_experiment_things
  where experiment_id=-1
    and json_unquote(metadata_json->'$.sync_object.user_name') in ('Bachounda', 'Amr F.Nagy', 'Avicenno', 'Omar kandil', 'Mohammad hajeer', 'علاء الدين');
