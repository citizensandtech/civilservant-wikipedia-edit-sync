import os
import sqlalchemy

import pandas as pd
from pathlib import Path

import civilservant.logs
import click
from civilservant.db import init_session
from civilservant.models.core import ExperimentThing
import yaml
from civilservant.util import PlatformType, ThingType
from civilservant.wikipedia.queries.users import normalize_user_name_get_user_id_api

civilservant.logs.initialize()
import logging


class randomizationUploader():
    def __init__(self, config_file, fn):
        """groups needing edits and size N edits to be included which k edits to be displayed
        """
        config = yaml.safe_load(open(os.path.join(Path(__file__).parent.parent, 'config', config_file), 'r'))
        self.fn = fn
        self.config = config
        self.db_session = init_session()
        self.inital_num_experiment_things = self.num_experiment_things()
        self.df = None
        self.ets_to_add = []

    def num_experiment_things(self):
        return self.db_session.query(ExperimentThing).count()

    def read_input(self):
        self.randomizations_f = os.path.join(self.config['project_dir'], self.config['randomizations_dir'],
                                             self.config['randomizations_file'])
        logging.info(f"trying to read {self.randomizations_f}")
        df = pd.read_csv(self.randomizations_f)
        df = df.rename(columns={'prev_experience': 'user_experience_level'})
        self.df = df

    def normalize_user_names(self):
        self.df['user_name_resp'] = self.df[['user_name','lang']].apply(
            lambda row: normalize_user_name_get_user_id_api(user_name=row['user_name'], lang=row['lang']), axis=1)
        self.df['user_name'] = self.df['user_name_resp'].apply(lambda d: d['name'])
        logging.debug(f"name normalization responses were: {self.df['user_name_resp']}")

    def upload(self, cols_to_save, thanker_thankee):

        for i, row in self.df.iterrows():
            row = row.fillna(0)
            row_map = {c: row[c] for c in cols_to_save}
            if thanker_thankee == 'superthankers':
                experiment_id = -1
                et_id = f'user_name:{row["lang"]}:{row["user_name"]}'
                randomization_condition = "superthanker"
                randomization_arm = None
                syncable = False
                randomization_block_id = -1
                randomization_block_size = -1

            if thanker_thankee == 'thankers':
                # THANKER details
                experiment_id = -1
                et_id = f'user_name:{row["lang"]}:{row["user_name"]}'
                # backwards compatibility
                if not ("superthanker" in row.keys()):
                    row['superthanker'] = False
                if row["superthanker"] == True:
                    randomization_condition = "superthanker"
                    randomization_arm = None
                else:
                    randomization_condition = "main"
                    randomization_arm = row["randomization_arm"]
                syncable = False
                randomization_block_id = row['randomization_block_id']
                randomization_block_size = row['randomization_block_size']
            elif thanker_thankee == 'thankees':
                # THANKEE details
                # first check that this thankee is in the thanking randomization condition
                randomization_arm = row["randomization_arm"]
                assert randomization_arm in [0, 1], "randomization arm needs to be 0 or 1"
                et_id = f'user_id:{row["lang"]}:{row["user_id"]}'
                if randomization_arm == 0:
                    logging.info(f"Not making an ET for {et_id} because their randomization arm is {randomization_arm}")
                    continue
                experiment_id = -3
                randomization_condition = 'thankee'
                syncable = True
                randomization_block_id = row['randomization_block_id']
                randomization_block_size = row['randomization_block_size']

            logging.info(f'attempting id {et_id}')
            existing_id_record = self.db_session.query(ExperimentThing).filter(
                ExperimentThing.id == et_id).one_or_none()
            if existing_id_record:
                logging.info(f"found existing entry for {et_id} nothing to do")
                continue
            else:
                et = ExperimentThing(
                    id=et_id,
                    thing_id="not_in_use", # row["anonymized_id"],
                    experiment_id=experiment_id,
                    randomization_condition=randomization_condition,
                    randomization_arm=randomization_arm,
                    object_platform=PlatformType.WIKIPEDIA,
                    object_type=ThingType.WIKIPEDIA_USER,
                    object_created_dt=None,
                    query_index=None,
                    syncable=syncable,
                    synced_dt=None,
                    metadata_json={'sync_object': row_map,
                                   'randomization_block_id':randomization_block_id,
                                   'randomization_block_size':randomization_block_size})
                self.ets_to_add.append(et)
            self.db_session.add_all(self.ets_to_add)
            self.db_session.commit()

            # except (sqlalchemy.exc.OperationalError, sqlalchemy.exc.IntegrityError):
            #     logging.info(f"error row: {row}")
            #     logging.info(row_map)
            #     self.db_session.rollback()

    def confirm_upload(self):
        curr_num_experiment_things = self.num_experiment_things()
        logging.info(
            f'experiment things. initially {self.inital_num_experiment_things}, added {len(self.ets_to_add)}, ended {curr_num_experiment_things}')
        assert self.inital_num_experiment_things + len(self.ets_to_add) == curr_num_experiment_things

    def run(self, fn):
        logging.info(f"fn is {fn}")
        if fn == 'thankers':
            cols_to_save = self.config['cols_to_save']
            self.read_input()
            self.upload(cols_to_save, thanker_thankee=fn)
            self.confirm_upload()
        if fn == 'thankees':
            cols_to_save = self.config['cols_to_save']
            self.read_input()
            self.upload(cols_to_save, thanker_thankee=fn)
            self.confirm_upload()
        if fn == 'superthankers':
            cols_to_save = self.config['cols_to_save']
            self.read_input()
            self.normalize_user_names()
            self.upload(cols_to_save, thanker_thankee=fn)
            self.confirm_upload()


@click.command()
@click.option("--fn", default="thankers", help="the portion to run")
@click.option('--config', default="randomizations_uploader_thanker.yaml", help='the config file to use')
def run_onboard(fn, config):
    # config_file = os.getenv('ONBOARDER_CONFIG', config)
    uploader = randomizationUploader(config, fn)
    assert fn in ['thankers', 'thankees', 'superthankers'], "fn must be one of 'thanker' or 'thankee'"
    uploader.run(fn)


if __name__ == "__main__":
    logging.info("Starting randomization uploader")
    run_onboard()
    logging.info("End randomization uploader")
