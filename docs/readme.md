#To install

## create a wikimedia developer account
https://wikitech.wikimedia.org/w/index.php?title=Special:CreateAccount
- upload ssh key through Prefecences--openstack
- make user a member of a VPS project (this should give them bastion)

## setting up the os environ
- install python 3 from source https://realpython.com/installing-python/
- sudo pip3 install --upgrade pip
- pip3 install --user pipenv
- add PATH="/home/maximilianklein/.local/bin:$PATH" to .profile and re-source

## getting pipenv install to work
- pipenv will hang if it tries to install a repo from github and the shell requires prompts. so you will have to get git credentials
into a state in which doing git clone https://github.com/mitmedialab/civilservantlib-wikipedia will not require password prompt. (yes you have to use https)
the way to do this is to use git credential helper. trigger the password prompt and allow it to remember. don't proceeed untill you
can `git clone https://github.com/mitmedialab/civilservantlib-wikipedia` without password prompt

- cd /src/editsync
- pipenv install
- cs init
- copy over the real .env file, "core" version
- copy over alembic.ini


## mysql
- create user
- grant privileges
- `ALTER TABLE gratitude_edit_sync.edits CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;`


## adding
- PIPENV_DOTENV_LOCATION=.grat.env pipenv shell
- alembic upgrade head
- PIPENV_DOTENV_LOCATION=.core.env pipenv shell
- cs db.upgrade

## since last sync
- things that weren't in pipfile: pandas mwdb
- after pipenv installing them, branch changes overwritten on civilservant-wikipedia
- how to properly import from data_gathering_jobs
- db.py ?charset=utf8, encoding="utf-8"

## installing redis
- sudo apt-get install redis-server
- sudo systemctl enable redis-server.service

## .env changes
- make sure wmf ports are correct

## logging
- logging just to stdout, not logfile. some bug?

# Running onboarder thankee
1. python `editsync/onboard_thankees.py --fn run --config configfile`
1. with pipenv shell, also need to run an rq worker. `pipenv shell rq worker onboarder_thankee`
    1. can add mutliple workers to speed things up.
