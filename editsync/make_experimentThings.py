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

    def num_experiment_things(self):
        return self.db_session.query(ExperimentThing).count()

    def read_input(self):
        self.randomizations_f = os.path.join(self.config['project_dir'], self.config['randomizations_dir'],
                                             self.config['randomizations_file'])
        self.df = pd.read_csv(self.randomizations_f)

    def upload(self, cols_to_save, thanker_thankee):
        self.ets_to_add = []
        for i, row in self.df.iterrows():
            row = row.fillna(0)
            row_map = {c: row[c] for c in cols_to_save}
            et_id = f'user_name:{row["lang"]}:{row["user_name"]}'
            logging.info(f'attempting id {et_id}')
            existing_id_record = self.db_session.query(ExperimentThing).filter(
                ExperimentThing.id == et_id).one_or_none()
            if existing_id_record:
                continue
            else:
                et = ExperimentThing(
                    id=et_id,
                    thing_id=row["anonymized_id"],
                    experiment_id=-1 if thanker_thankee == 'thanker' else -2,
                    randomization_condition='main',
                    randomization_arm=row["randomization_arm"],
                    object_platform=PlatformType.WIKIPEDIA,
                    object_type=ThingType.WIKIPEDIA_USER,
                    object_created_dt=None,
                    query_index=None,
                    syncable=True,
                    synced_dt=None,
                    metadata_json=row_map)
                self.ets_to_add.append(et)
            self.db_session.add_all(self.ets_to_add)
            self.db_session.commit()

            # except (sqlalchemy.exc.OperationalError, sqlalchemy.exc.IntegrityError):
            #     logging.info(f"error row: {row}")
            #     logging.info(row_map)
            #     self.db_session.rollback()

    def confirm_upload(self):
        curr_num_experiment_things = self.num_experiment_things()
        logging.info(f'experiment things. initially {self.inital_num_experiment_things}, added {len(self.ets_to_add)}, ended {curr_num_experiment_things}')
        assert self.inital_num_experiment_things + len(self.ets_to_add) == curr_num_experiment_things

    def run(self, fn):
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


@click.command()
@click.option("--fn", default="thankers", help="the portion to run")
@click.option('--config', default="randomizations_uploader_thanker.yaml", help='the config file to use')
def run_onboard(fn, config):
    # config_file = os.getenv('ONBOARDER_CONFIG', config)
    uploader = randomizationUploader(config, fn)
    uploader.run(fn)


if __name__ == "__main__":
    logging.info("Starting randomization uploader")
    run_onboard()
