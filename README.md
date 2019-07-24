civilservant-wikipedia-edit-sync
================================

#setup


# create a wikimedia developer account
https://wikitech.wikimedia.org/w/index.php?title=Special:CreateAccount
- upload ssh key through Prefecences--openstack
- make user a member of a VPS project (this should give them bastion)
-

# setting up the os environ
- install python 3 from source https://realpython.com/installing-python/
- sudo pip3 install --upgrade pip
- pip3 install --user pipenv

# install pipenv
- add PATH="/home/maximilianklein/.local/bin:$PATH" to .profile and re-source



# mysql
we need mysql-server and client with version 8
- wget http://repo.mysql.com/mysql-apt-config_0.8.13-1_all.deb
- sudo dpkg -i mysql-apt-config_0.8.13-1_all.deb
   -- navigate this and make sure to select v 8
- sudo apt update
- sudo apt install msyql-server
  -- record root password

- sudo apt-get install libmysqlclient-dev

- create user 'gratitude' identified by 'pass';
- create database civilservant_production;
- grant all privileges on civilservant.production to 'gratitude';

# being a production user account
- sudo adduser civilservant
- sudo -i && sudo -Hiu civilservant

## getting pipenv install to work
- pipenv will hang if it tries to install a repo from github and the shell requires prompts. so you will have to get git credentials
into a state in which doing git clone https://github.com/mitmedialab/civilservantlib-wikipedia will not require password prompt. (yes you have to use https)
the way to do this is to use git credential helper. trigger the password prompt and allow it to remember. don't proceeed untill you
can `git clone https://github.com/mitmedialab/civilservantlib-wikipedia` without password prompt

# cacheing password
- git config credential.helper 'cache --timeout=3600'
- then somewhere else do  `git clone https://github.com/mitmedialab/civilservantlib-wikipedia` and enter in

- cd /src/editsync
- pipenv install
    -- need to pip install mwdb in a pipenv shell after
- cs init
- copy over the real .env file, "core" version
- copy over alembic.ini


# database migrations
- PIPENV_DOTENV_LOCATION=.grat.env pipenv shell (so that CS_DB_ALEMBIC_TABLE='alembic_version_cs_gratitude')
- alembic upgrade head
- PIPENV_DOTENV_LOCATION=.core.env pipenv shell (so that CS_DB_ALEMBIC_TABLE='alembic_version_cs_core')
- cs db.upgrade or [ cd lib/civilservant-core && alembic upgrade head ]
- alter table edits convert to character set utf8mb4 collate utf8mb4_general_ci;

# installing redis
- sudo apt-get install redis-server
- sudo systemctl enable redis-server.service
- in a screen tab and inside a pipenv shell and from the editsync/editsync folder `rqworker onboarder_thankee &`


## since last sync
- things that weren't in pipfile: pandas mwdb
----- after pipenv installing them, branch changes overwritten on civilservant-wikipedia
- how to properly import from data_gathering_jobs
- db.py ?charset=utf8, encoding="utf-8"


# .env changes
- make sure wmf ports are correct




### data specific notes
Meaning of Experiment ID's of experiment things
+ -1 = thanker 
  + randomizaton_arm=0, randomization_cond='main' --> activity,
  + randomization_arm=1, randomization_cond='main'--> thanker,
  + randomization_arm=NULL, randomization_cond='superthanker' --> superthanker
+ -3 = thankee (candidate) SYNC_OBJ=WIKIPEDIA_USER
+ -10 = thankee (edit)  SYNC_OBJ=WIKIPEDIA_EDIT


### How to exclude an ExperimentThing so it wont be unpacked 
to avoid having your experimentThing trying to be unpacked by `cs modelsync.unpack`
set:
+ syncable=False
+ synced_dt=NULL
