import os
import pandas as pd

import click
from civilservant.wikipedia.connections.database import make_wmf_con
import civilservant.logs

import yaml
civilservant.logs.initialize()
import logging

class thankerOnboarder():
    def __init__(self, config_file):
        """groups needing edits and size N edits to be included which k edits to be displayed
        """
        config = yaml.safe_load(open(os.path.join('config', config_file), 'r'))
        self.config = config
        self.langs = config['langs']
        self.wmf_con = make_wmf_con()

    def read_input_thankees(self):
        for lang in self.langs:
            users_filename = self.config['langs'][lang]['survey_file']
            thankers = pd.read_csv(users_filename)
        # user name normalization _takeout spaces, capitalize?

    def run(self, fn):
        if fn == 'run':
            self.read_input_thankees()
            self.make_thankee_historical_data()
            self.merge_historical_and_survey_data()
            self.write_result()

@click.command()
@click.option("--fn", default="run", help="the portion to run")
def run_onboard(fn):
    config_file = os.getenv('ONBOARDER_CONFIG', 'onboarder_thanker.yaml')
    onboarder = thankerOnboarder(config_file)
    onboarder.run(fn)


if __name__ == "__main__":
    logging.info("Starting Oboarder")
    run_onboard()
