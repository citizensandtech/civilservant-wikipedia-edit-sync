civilservant-wikipedia-edit-sync
================================

## data specific notes
Meaning of Experiment ID's of experiment things
+ -1 = thanker 
  + randomizaton_arm=0, randomization_cond='main' --> activity,
  + randomization_arm=1, randomization_cond='main'--> thanker,
  + randomization_arm=NULL, randomization_cond='superthanker' --> superthanker
+ -3 = thankee (candidate) SYNC_OBJ=WIKIPEDIA_USER
+ -10 = thankee (edit)  SYNC_OBJ=WIKIPEDIA_EDIT


## How to exclude an ExperimentThing so it wont be unpacked 
to avoid having your experimentThing trying to be unpacked by `cs modelsync.unpack`
set:
+ syncable=False
+ synced_dt=NULL
