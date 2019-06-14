import datetime
import os
from pathlib import Path

import mwapi
import mwdb
import pandas as pd

import click
from civilservant.wikipedia.connections.database import make_wmf_con

import yaml
from civilservant.wikipedia.queries.user_interactions import get_bans, get_num_revertings, \
    get_thanks_sending, get_wikiloves_sending, get_user_disablemail_properties
from civilservant.wikipedia.queries.users import normalize_user_name_get_user_id_api, get_user_basic_data, \
    get_user_edits, get_official_bots

import civilservant.logs
from civilservant.wikipedia.utils import get_namespace_fn

civilservant.logs.initialize()
import logging

class thankerOnboarder():
    def __init__(self, config_file):
        """groups needing edits and size N edits to be included which k edits to be displayed
        """
        config = yaml.safe_load(open(os.path.join(Path(__file__).parent.parent, 'config', config_file), 'r'))
        self.config = config
        self.langs = config['langs']
        self.experiment_start_date = config['experiment_start_date']
        self.observation_start_date = self.experiment_start_date - datetime.timedelta(config['observation_back_days'])
        self.mwapi_sessions = {lang: self.make_mwapi_session(lang) for lang in self.langs}
        self.wmf_con = make_wmf_con()
        self.thankers = {}
        self.surveys = {}
        self.merged = {}
        self.superthankers = {}

        self.qualtrics_map = yaml.safe_load(open(os.path.join(Path(__file__).parent.parent, 'config', "qualtrics_to_interal_field_map.yaml"), 'r'))


    def make_mwapi_session(self, lang):
        return mwapi.Session(f'https://{lang}.wikipedia.org', user_agent="CivilServant thanker-onboarder <max@civilservant.io>")


    def read_user_input(self, lang, input_type):
        survey_filename = self.config['langs'][lang][input_type]
        survey_file = os.path.join(self.config['dirs']['project'], self.config['dirs']['input'], survey_filename)
        df = pd.read_csv(survey_file)
        if input_type == 'consented_file':
            df['user_name_resp'] = df['user_name'].apply(lambda u: normalize_user_name_get_user_id_api(user_name=u, mwapi_session=self.mwapi_sessions[lang]))
            df['user_name'] = df['user_name_resp'].apply(lambda d: d['name'])
            df['user_id'] = df['user_name_resp'].apply(lambda d: d['userid'] if 'userid' in d.keys() else -1)
            assert df['user_id'].dtype == pd.np.int64
            df['lang'] = df['lang'].apply(lambda s: s.lower())

            if 'lang' not in df.columns:
                df['lang'] = lang

            unresolvable = df[df['user_id']<0]
            if len(unresolvable) > 0:
                self.write_output(output_dir=f'{input_type}_unresolvable', output_df_dict=None, lang=lang, fname_extra='unresolvable_ids', df_to_write=unresolvable)

            df = df[df['user_id']>0]
            del df['user_name_resp']

        elif input_type == 'survey_file':
            pass
        return df

    def read_survey_input(self, lang):
        sf = self.read_user_input(lang, 'survey_file')
        self.surveys[lang] = sf

    def read_input_thankers(self, lang):
        df = self.read_user_input(lang, 'consented_file')
        self.thankers[lang] = df

    def read_superthankner_input(self, lang):
        st = self.read_user_input(lang, 'superthanker_file')
        self.superthankers[lang] = st

    def read_midstage_dir(self, lang, mistage_dir, dict_to_load):
        hist_dir = os.listdir(os.path.join(self.config['dirs']['project'], self.config['dirs'][mistage_dir]))
        lang_fs = [f for f in hist_dir if f.startswith(lang)]
        f = max(sorted(lang_fs, key=lambda fname: datetime.datetime.strptime(fname.split('.csv')[0].split("-")[2], '%Y%m%d')))
        logging.info(f'found {len(lang_fs)} {mistage_dir} files for {lang}. most recent is {f}')
        dict_to_load[lang] = pd.read_csv(os.path.join(self.config['dirs']['project'], self.config['dirs'][mistage_dir], f))

    def read_historical_output(self, lang):
        self.read_midstage_dir(lang, mistage_dir='historical_output', dict_to_load=self.thankers)

    def read_merged_survey_output(self, lang):
        self.read_midstage_dir(lang, mistage_dir='merged_output', dict_to_load=self.merged)

    def add_user_basic_data(self, df, lang):
        users_basic_data = []
        for user_id in df['user_id'].values:
            # user_basic_data = get_user_basic_data(lang, user_name=user_name, wmf_con=self.wmf_con)
            user_basic_data = get_user_basic_data(lang, user_id=user_id, wmf_con=self.wmf_con)
            users_basic_data.append(user_basic_data)

        demographics = pd.concat(users_basic_data)

        df = pd.merge(df, demographics, on=['user_name', 'lang'], suffixes=("","_basic_data"))
        return df


    def add_blocks(self, df, lang, start_date=None, end_date=None, col_label="block_actions_84_pre_treatment"):
        if start_date is None:
            start_date = self.observation_start_date
        if end_date is None:
            end_date = self.experiment_start_date
        bans = get_bans(lang, start_date, end_date, wmf_con=self.wmf_con)
        bans = bans.rename(columns={'blocking_user_id': 'user_id'})
        user_ban_counts = pd.DataFrame(bans.groupby(['lang', 'user_id']).size()).reset_index()
        user_ban_counts['user_id'] = user_ban_counts['user_id'].apply(int)
        logging.info(f"There are {len(user_ban_counts)} banning users.")
        df = pd.merge(df, user_ban_counts, on=['user_id', 'lang'], how='left').rename(columns={0: col_label})
        df[col_label] = df[col_label].fillna(0)
        return df

    def add_bots(self, df, lang):
        bots = get_official_bots(lang=lang, wmf_con=self.wmf_con)
        logging.info(f"Found {len(bots)} official bots on {lang}")
        df = pd.merge(df, bots, on=['user_id', 'lang'], how='left')
        df['is_official_bot'] = df['is_official_bot'].fillna(False)
        df['is_official_bot'] = df['is_official_bot'].apply(bool)
        return df


    def add_reverting_actions(self, df, lang):
        user_revert_dfs = []
        schema = mwdb.Schema(f"mysql+pymysql://{os.getenv('WMF_MYSQL_HOST')}:{os.getenv('WMF_MYSQL_PORT')}/{lang}wiki_p?read_default_file=~/replica.my.cnf",
                             only_tables=['revision'], pool_size=9, max_overflow=0)
        for user_id in df['user_id'].values:
            self.wmf_con = make_wmf_con()
            user_df = get_user_edits(lang, user_id, self.observation_start_date, self.experiment_start_date, wmf_con=self.wmf_con)
            rev_ids = user_df['rev_id'].values
            #TODO  undo this limitation when we're really in production
            # rev_ids = rev_ids[:10]
            logging.info(f"User {lang}:{user_id}, has {len(rev_ids)} revs between {self.observation_start_date} and {self.experiment_start_date}")
            user_revert_df = get_num_revertings(lang, user_id, rev_ids, schema=schema, db_or_api='db')
            user_revert_dfs.append(user_revert_df)

        user_reverts = pd.concat(user_revert_dfs)
        return pd.merge(df, user_reverts, on=['user_id', 'lang'])

    def get_talk_counts(self, user_id, user_df, namespace_fn, col_label):
        talk_count = user_df['page_namespace'].apply(namespace_fn).sum()
        user_talk_df = pd.DataFrame.from_dict({col_label: [talk_count],
                                               'user_id': [user_id]}, orient='columns')
        return user_talk_df

    def create_talk_df(self, df, start_date=None, end_date=None, namespace_fn=None, lang=None, col_label=None):
        start_date = self.observation_start_date if start_date is None else start_date
        end_date = self.experiment_start_date if end_date is None else end_date

        talk_dfs = []
        user_ids = df['user_id'].values
        for user_id in user_ids:
            user_df = get_user_edits(lang, user_id, start_date, end_date, wmf_con=self.wmf_con)
            user_talk_df = self.get_talk_counts(user_id, user_df, namespace_fn, col_label)
            talk_dfs.append(user_talk_df)

        talk_df = pd.concat(talk_dfs)
        df = pd.merge(df, talk_df, how='left', on=['user_id'])
        return df

    def add_support_talk(self, df, lang):
        return self.create_talk_df(df, namespace_fn=get_namespace_fn('talk'), lang=lang, col_label='support_talk_84_pre_treatment')

    def add_project_talk(self, df, lang):
        return self.create_talk_df(df, namespace_fn=get_namespace_fn('project'), lang=lang, col_label='project_talk_84_pre_treatment')


    def add_thanks(self, df, lang):
        user_thank_count_dfs = []
        user_names = df['user_name'].values
        for user_name in user_names:
            user_thank_df = get_thanks_sending(lang, user_name,
                                               start_date=self.observation_start_date,
                                               end_date=self.experiment_start_date,
                                               wmf_con=self.wmf_con)
            user_thank_count_df = pd.DataFrame.from_dict({'wikithank_84_pre_treatment': [len(user_thank_df)],
                                                          'user_name': [user_name],
                                                          'lang': [lang]}, orient='columns')
            user_thank_count_dfs.append(user_thank_count_df)

        thank_counts_df = pd.concat(user_thank_count_dfs)
        df = pd.merge(df, thank_counts_df, how='left', on=['user_name', 'lang'])
        return df

    def add_wikiloves(self, df, lang):
        user_wikilove_count_dfs = []
        user_ids = df['user_id'].values
        for user_id in user_ids:
            if lang not in ('de', 'pl'):
                user_wikilove_df = get_wikiloves_sending(lang, user_id,
                                                         start_date=self.observation_start_date,
                                                         end_date=self.experiment_start_date,
                                                         wmf_con=self.wmf_con)
                num_wikilove = len(user_wikilove_df)
            else:
                num_wikilove = float('nan')
            user_wikilove_count_df = pd.DataFrame.from_dict({'wikilove_84_pre_treatment': [num_wikilove],
                                                             'user_id': [user_id],
                                                             'lang': [lang]}, orient='columns')
            user_wikilove_count_dfs.append(user_wikilove_count_df)

        wikilove_counts_df = pd.concat(user_wikilove_count_dfs)
        df = pd.merge(df, wikilove_counts_df, how='left', on=['user_id', 'lang'])
        return df

    def add_has_email(self, df, lang):
        user_prop_dfs = []
        user_ids = df['user_id'].values
        logging.info(f'add has email looking at {len(user_ids)} users')
        for user_id in user_ids:
            user_prop_df = get_user_disablemail_properties(lang, user_id, self.wmf_con)
            has_email = False if len(
                user_prop_df) >= 1 else True  # the property disables email, if it doesn't exist the default its that it's on
            user_prop_dfs.append(pd.DataFrame.from_dict({'has_email': [has_email],
                                                         'user_id': [user_id],
                                                         'lang': [lang]}, orient='columns'))

        users_prop_df = pd.concat(user_prop_dfs)

        df = pd.merge(df, users_prop_df, how='left', on=['user_id', 'lang'])
        return df

    def make_thanker_historical_data(self, lang):
        df = self.thankers[lang]
        logging.info("starting to get database information")

        logging.info(f'adding reverts, shape of df is {df.shape}')
        df = self.add_reverting_actions(df, lang)

        logging.info(f'adding project talk, shape of df is {df.shape}')
        df = self.add_project_talk(df, lang)

        logging.info(f'adding support talk, shape of df is {df.shape}')
        df = self.add_support_talk(df, lang)

        logging.info(f'adding bots, shape of df is {df.shape}')
        df = self.add_bots(df, lang)


        logging.info(f'adding wikithanks, shape of df is {df.shape}')
        df = self.add_thanks(df, lang)

        logging.info(f'adding wikiloves, shape of df is {df.shape}')
        df = self.add_wikiloves(df, lang)

        logging.info(f'adding blocks, shape of df is {df.shape}')
        df = self.add_blocks(df, lang)


        logging.info(f'adding email, shape of df is {df.shape}')
        df = self.add_has_email(df, lang)

        logging.debug(df)
        self.thankers[lang] = df

    def write_historical_output(self, lang):
        self.write_output(self.config['dirs']['historical_output'], self.thankers, lang, "historical")

    def write_merged_survey_output(self, lang):
        self.write_output(self.config['dirs']['merged_output'], self.merged, lang, "merged")

    def write_excluded_superthankers_output(self, lang):
        self.write_output(self.config['dirs']['superthanker_merged_output'], self.merged, lang, "merged_no_superthankers")

        keys_sofar = self.superthankers.keys()
        # if we've computed every language
        if len(keys_sofar) == len(self.langs):
            final_df = pd.concat(self.merged.values())
            # TODO make this prettier
            final_cols = ['user_name',
                          'anonymized_id',
                          'user_id',
                           'num_reverts_84_pre_treatment',
                            'wikithank_84_pre_treatment',
                            'wikilove_84_pre_treatment',
                            'is_official_bot',
                            'blocking_actions_84_pre_treatment',
                            'project_talk_84_pre_treatment',
                            'support_talk_84_pre_treatment',
                            'has_email',
                            ]
            final_cols.extend(self.qualtrics_map.values())
            final_df = final_df[final_cols]
            self.write_output(output_dir=self.config['dirs']['superthanker_merged_output'], output_df_dict=None,
                              lang='all', fname_extra='merged_no_superthankers', df_to_write=final_df)

    def write_output(self, output_dir, output_df_dict, lang, fname_extra, df_to_write=None):
        out_df = output_df_dict[lang] if df_to_write is None else df_to_write

        out_fname = f"{lang}-{fname_extra}-{datetime.date.today().strftime('%Y%m%d')}.csv"
        out_base = os.path.join(self.config['dirs']['project'], output_dir)
        if not os.path.exists(out_base):
            os.makedirs(out_base, exist_ok=True)
        out_f = os.path.join(out_base, out_fname)
        out_df.to_csv(out_f, index=False)

    def merge_historical_and_survey_data(self, lang):
        t = self.thankers[lang]
        s = self.surveys[lang]
        s['lang'] = lang
        merged_df = pd.merge(t, s, how="left", on=['ID'], suffixes=("", "__survey"))
        merged_df = merged_df.rename(columns=self.qualtrics_map)
        self.merged[lang] = merged_df

    def exclude_superthankers(self, lang):
        merged = self.merged[lang]
        st = self.superthankers[lang]
        st['is_superthanker'] = True
        merged_st = pd.merge(merged, st, how='left', on=['user_name', 'lang'])
        merged_non_st = merged_st[pd.isnull(merged_st['is_superthanker'])]
        self.merged[lang] = merged_non_st

    def run(self, fn):
        for lang in self.langs:
            if fn == 'historical':
                self.make_mwapi_session(lang)
                self.read_input_thankers(lang)
                self.make_thanker_historical_data(lang)
                self.write_historical_output(lang)
            if fn == 'merge':
                self.read_historical_output(lang)
                self.read_survey_input(lang)
                self.merge_historical_and_survey_data(lang)
                self.write_merged_survey_output(lang)
            if fn == 'exclude_superthankers':
                self.read_merged_survey_output(lang)
                self.read_superthankner_input(lang)
                self.exclude_superthankers(lang)
                self.write_excluded_superthankers_output(lang)


@click.command()
@click.option("--fn", default="historical", help="the portion to run")
@click.option('--config', default="onboarder_thanker.yaml", help='the config file to use')
def run_onboard(fn, config):
    # config_file = os.getenv('ONBOARDER_CONFIG', config)
    onboarder = thankerOnboarder(config)
    onboarder.run(fn)


if __name__ == "__main__":
    logging.info("Starting Oboarder")
    run_onboard()
