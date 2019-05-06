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
        self.randomizations_f = os.path.join(self.config['project_dir'], self.config['randomizations_dir'], self.config['randomizations_file'])
        self.df = pd.read_csv(self.randomizations_f)

    def upload(self):
        for i, row in self.df.iterrows():
            row = row.fillna(0)
            row_dict = {'user_name': row['user_name'],
                        'anonymized_id': row['anonymized_id'],
                        'user_id': row['user_id'],
                        'num_reverts_84_pre_treatment': row['num_reverts_84_pre_treatment'],
                        'wikithank_84_pre_treatment': row['wikithank_84_pre_treatment'],
                        'wikilove_84_pre_treatment': row['wikilove_84_pre_treatment'],
                        'is_official_bot': row['is_official_bot'],
                        'block_actions_84_pre_treatment': row['block_actions_84_pre_treatment'],
                        'project_talk_84_pre_treatment': row['project_talk_84_pre_treatment'],
                        'support_talk_84_pre_treatment': row['support_talk_84_pre_treatment'],
                        'has_email': row['has_email'],
                        'pre_newcomer_capability': row['pre_newcomer_capability'],
                        'pre_newcomer_intent': row['pre_newcomer_intent'],
                        'pre_emotionally_draining': row['pre_emotionally_draining'],
                        'pre_feel_positive': row['pre_feel_positive'],
                        'pre_monitoring_damaging_content': row['pre_monitoring_damaging_content'],
                        'pre_mentoring': row['pre_mentoring'],
                        'lang': row['lang'],
                        'supportive_84_pre_treatment': row['supportive_84_pre_treatment'],
                        'protective_84_pre_treatment': row['protective_84_pre_treatment'],
                        'randomization_protectiveness_index': row['randomization_protectiveness_index'],
                        'randomization_block_id': row['randomization_block_id'],
                        'randomization_block_size': row['randomization_block_size']}

            et = ExperimentThing(
                            id=f'{row["lang"]}:{row["user_id"]}]',
                            thing_id=row['anonymized_id'],
                            experiment_id = -1,
                            randomization_condition = 'main',
                            randomization_arm = row['randomization_arm'],
                            object_platform = PlatformType.WIKIPEDIA,
                            object_type = ThingType.WIKIPEDIA_USER,
                            object_created_dt = None,
                            query_index = None,
                            syncable = True,
                            synced_dt = None,
                            metadata_json = row_dict)
            self.db_session.add(et)
            self.db_session.commit()

            # except (sqlalchemy.exc.OperationalError, sqlalchemy.exc.IntegrityError):
            #     logging.info(f"error row: {row}")
            #     logging.info(row_dict)
            #     self.db_session.rollback()

    def confirm_upload(self):
        curr_num_experiment_things = self.num_experiment_things()
        assert self.inital_num_experiment_things + len(self.df) == curr_num_experiment_things


    def run(self, fn):
        if fn == 'thankers':
            self.read_input()
            self.upload()
            self.confirm_upload()


@click.command()
@click.option("--fn", default="thankers", help="the portion to run")
@click.option('--config', default="randomizations_uploader.yaml", help='the config file to use')
def run_onboard(fn, config):
    # config_file = os.getenv('ONBOARDER_CONFIG', config)
    uploader = randomizationUploader(config, fn)
    uploader.run(fn)


if __name__ == "__main__":
    logging.info("Starting randomization uploader")
    run_onboard()





