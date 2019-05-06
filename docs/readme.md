# Running onboarder thankee
1. python `editsync/onboard_thankees.py --fn run --config configfile`
1. with pipenv shell, also need to run an rq worker. `pipenv shell rq worker onboarder_thankee`
    1. can add mutliple workers to speed things up.
