# Data Model

## Candidate (a user who might be thanked)
### 1-1 with wikipedia users if updating, many-1 if inserting not updating
+ cand_id (internal)
+ created_at (when the record was created)
+ lang (wikipedia code)
+ user_id (wikipedia user id unique to language)
+ user_name (wikipedia user name unique to language)
+ user_reg (datetime when the wikipedia account was created)
+ edit_count (num edits on wiki)
+ experience_level (enum[newcomer, or experienced])
+ thanks_sent (num thanks sent on wiki)
+ thanks_received (num thanks received on wiki)
+ has_email (user can be emailed via wiki)
+ user_thanked (whether this user has been thanked yet)

## randomizations
### many-1 to candidate
+ randomization_id (internal)
+ cand_id (foreign key)
+ condition (enum[not-in-experiment, thankee, placebo])
+ condition_created_at (date)

## edits
+ edit_id (internal)
+ lang
+ rev_id (wikipedia id unique to lang)
+ page_name
+ page_id
+ page_namespace
+ ores_damaging
+ ores_goodfaith
+ ores_damaging_score
+ ores_goodfaith_score
+ de_flagged (de-only)
+ de_flagging_algo_version 


## edits_display
+ edit_id (internal)
+ edit_deleted (bool, if true then null other data)
+ diffHTML
+ lang (redundant, could not store)
+ newRevId (redundant, could not store)
+ newRevDate (redundant, could not store)
+ newRevUser (redundant, could not store)
+ newRevComment
+ oldRevId
+ oldRevDate
+ oldRevUser
+ oldRevComment

## worksets
###
+ workset_id
+ task_id

## tasks
### a collection of edits for thanking consideration
+ task_id
+ cand_id
+ edits (json list) (how to make dynamic in case of skipping?)

##  task_responses
+ task_response_id (internal)
+ task_id
+ responder_id (wikipedia user id)
+ action (enum[editid, or "SKIP"])
