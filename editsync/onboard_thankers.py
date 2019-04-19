import datetime
import os
from pathlib import Path

import mwapi
import pandas as pd

import click
from civilservant.wikipedia.connections.database import make_wmf_con

import yaml
from civilservant.wikipedia.queries.user_interactions import get_bans
from civilservant.wikipedia.queries.users import normalize_user_name_get_user_id_api, get_user_basic_data

import civilservant.logs
from civilservant.wikipedia.utils import WIKIPEDIA_START_DATE

civilservant.logs.initialize()
import logging

class thankerOnboarder():
    def __init__(self, config_file):
        """groups needing edits and size N edits to be included which k edits to be displayed
        """
        config = yaml.safe_load(open(os.path.join(Path(__file__).parent.parent, 'config', config_file), 'r'))
        self.config = config
        self.langs = config['langs']
        self.mwapi_sessions = {lang: self.make_mwapi_session(lang) for lang in self.langs}
        self.wmf_con = make_wmf_con()
        self.thankers = {}

    def make_mwapi_session(self, lang):
        return mwapi.Session(f'https://{lang}.wikipedia.org', user_agent="CivilServant thanker-onboarder <max@civilservant.io>")

    def read_input_thankees(self, lang):
        users_filename = self.config['langs'][lang]['survey_file']
        df = pd.read_csv(users_filename)
        # user name normalization _takeout spaces, capitalize?
        df['user_name_normalization'] = df['user_name'].apply(lambda u: normalize_user_name_get_user_id_api(user_name=u, mwapi_session=self.mwapi_sessions[lang]))
        self.thankers[lang] = df

    def add_user_basic_data(self, df, lang):
        users_basic_data = []
        for user_name in df['user_name'].values:
            user_basic_data = get_user_basic_data(lang, user_name=user_name, wmf_con=self.wmf_con)
            users_basic_data.append(user_basic_data)

        demographics = pd.concat(users_basic_data)

        df = pd.merge(df, demographics, on='user_name')
        return df


    def add_blocks(self, df, lang, start_date=None, end_date=None, col_label="blocking_actions"):
        if start_date is None:
            start_date = WIKIPEDIA_START_DATE
        if end_date is None:
            end_date = datetime.datetime.utcnow()
        bans = get_bans(lang, start_date, end_date, wmf_con=self.wmf_con)
        bans = bans.rename(columns={'blocking_user_id': 'user_id'})
        user_ban_counts = pd.DataFrame(bans.groupby(['lang', 'user_id']).size()).reset_index()
        user_ban_counts['user_id'] = user_ban_counts['user_id'].apply(int)

        df = pd.merge(df, user_ban_counts, on=['lang', 'user_id'], how='left').rename(columns={0: col_label})
        df[col_label] = df[col_label].fillna(0)
        return df

    def make_thankee_historical_data(self, lang):
        df = self.thankers[lang]
        logging.info("starting to get database information")

        logging.info('adding blocks')
        df = self.add_user_basic_data(df, lang)

        logging.info('adding blocks')
        df = self.add_blocks(df, lang)
        #
        # logging.info('adding reverts')
        # df = add_revert_actions_pre_treatment(df)
        #
        # logging.info('adding support talk')
        # df = add_support_talk_90_pre_treatment(df)
        #
        # logging.info('adding project talk')
        # df = add_project_talk_90_pre_treatment(df)
        #
        # logging.info('adding wikithanks')
        # df = add_thanks_90_pre_treatment(df)
        #
        # logging.info('adding wikiloves')
        # df = add_wikilove_90_pre_treatment(df)
        self.thankers[lang] = df


    def run(self, fn):
        for lang in self.langs:
            if fn == 'run':
                self.make_mwapi_session(lang)
                self.read_input_thankees(lang)
                self.make_thankee_historical_data(lang)
                self.merge_historical_and_survey_data(lang)
                self.write_result(lang)

@click.command()
@click.option("--fn", default="run", help="the portion to run")
@click.option('--config', default="onboarder_thanker.yaml", help='the config file to use')
def run_onboard(fn, config):
    # config_file = os.getenv('ONBOARDER_CONFIG', config)
    onboarder = thankerOnboarder(config)
    onboarder.run(fn)


if __name__ == "__main__":
    logging.info("Starting Oboarder")
    run_onboard()
