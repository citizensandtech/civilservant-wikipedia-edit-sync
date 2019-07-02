import inspect
import os
import sqlalchemy
import time
from collections import defaultdict
from pathlib import Path

import click
import pandas as pd
import yaml
import civilservant.logs
from civilservant.util import PlatformType, ThingType
from civilservant.wikipedia.queries.revisions import get_quality_edits_of_users, get_display_data
from civilservant.wikipedia.queries.users import get_active_users
# from editsync.data_gathering_jobs import add_num_quality_user, add_has_email, add_thanks_receiving, add_labour_hours
from data_gathering_jobs import add_num_quality_user, add_has_email, add_thanks_receiving, add_labour_hours

civilservant.logs.initialize()
import logging
from civilservant.db import init_session
import civilservant.models.core

from civilservant.wikipedia.connections.database import make_wmf_con
from civilservant.models.wikipedia.thankees import candidates, edits
from civilservant.models.core import ExperimentThing
from civilservant.wikipedia.utils import to_wmftimestamp, from_wmftimestamp, decode_or_nan, add_experience_bin, \
    WIKIPEDIA_START_DATE, get_namespace_fn, THANK_FEATURE_INTRODUCITON

from datetime import timedelta, datetime, date

from redis import Redis
from rq import Queue


class thankeeOnboarder():
    def __init__(self, config_file, get_active_users_replacement=None, db_session_replacement=None):
        """groups needing edits and size N edits to be included which k edits to be displayed
        """
        config = yaml.safe_load(open(os.path.join(Path(__file__).parent.parent, 'config', config_file), 'r'))
        self.config = config
        self.langs = config['langs']
        self.min_edit_count = config['min_edit_count']
        self.wmf_con = make_wmf_con()
        self.db_session = init_session() if not db_session_replacement else db_session_replacement
        self.experiment_start_date = config['experiment_start_date']
        self.onboarding_earliest_active_date = self.experiment_start_date - timedelta(days=config['observation_back_days'])
        self.onboarding_latest_active_date = datetime.utcnow()
        self.populations = defaultdict(dict)
        self.namespace_fn = get_namespace_fn(config['namespace_fn'])
        self.get_active_users = get_active_users if not get_active_users_replacement else get_active_users_replacement

        if 'max_onboarders_to_check' in self.config.keys():
            self.max_onboarders_to_check = self.config['max_onboarders_to_check']
        else:
            self.max_onboarders_to_check = None

        self.users_in_thanker_experiment = {"ar": [], "de": [], "fa": [], "pl": [], }

        self.q = Queue(name='onboarder_thankee', connection=Redis())
        self.failed_q = Queue(name='failed', connection=Redis())

    def sample_population(self, lang):
        """
        - for incomplete groups:
        - sample active users
        - remove users with less than n edits
        - remove editors in thanker experiment
        - assign experience level (once only)
        - update/insert candidates
        - iterative representative sampling
        - add thanks history
        - add emailable status
        - add labour hours
        """
        # Get the active users
        active_users = self.get_active_users(lang, start_date=self.onboarding_earliest_active_date,
                                        end_date=self.onboarding_latest_active_date,
                                        min_rev_id=self.langs[lang]['min_rev_id'],
                                        wmf_con=self.wmf_con)
        # active_users.to_csv(f'active_users.{lang}.csv')
        # Subset to: - minimum edits
        active_users_min_edits = active_users[
            active_users['user_editcount'] >= self.min_edit_count]  # need to have at least this many edits
        # Subset to non-thanker experiment
        active_users_min_edits_nonthanker = active_users_min_edits[
            active_users_min_edits["user_id"].apply(lambda uid: uid not in self.users_in_thanker_experiment[lang])]
        # Add experience levels
        active_users_min_edits_nonthanker_exp = add_experience_bin(active_users_min_edits_nonthanker,
                                                                   self.experiment_start_date)
        logging.info(
            f"Group {lang} has {len(active_users_min_edits_nonthanker_exp)} active users with 4 edits in history.")

        # Now work on groups
        groups = self.config['langs'][lang]['groups']
        for group_name, inclusion_criteria in groups.items():
            df = self.get_quality_data_for_group(super_group=active_users_min_edits_nonthanker_exp,
                                                 lang=lang, group_name=group_name, inclusion_criteria=inclusion_criteria)

            ## Nota Bene. This is where things ge a bit wonky.
            # 1. at first I thought that I would store the user state in a candidates table, and in fact
            # that is useful for the sake of being able to multiprocess the quality-edits revision
            # however it is a pain to update columns in the grow-only right pandas-style, which the rest of the
            # independent variables. in addition since we aren't onboarding in a rolling-state, but once every
            # active-window-days, we don't really need to store the state to compare it. at ths point in collecting
            # data we switch to the pandas style and keep the user state is a dict of data frames "population".
            # So todo: reconcile the two ways to store state.
            # add previous thanks received last 90 /84

            # refereshing con here, sometimes gets stale after waiting
            self.wmf_con = make_wmf_con()
            self.db_session = init_session()

            logging.info('adding labour hours')
            if "labor_hours_84_days_pre_sample" not in df.columns:
                df = add_labour_hours(df, lang,
                                      start_date=self.onboarding_earliest_active_date, end_date=self.onboarding_latest_active_date,
                                      wmf_con=self.wmf_con, col_label="labor_hours_84_days_pre_sample")
                self.df_to_db_col(lang, df, 'labor_hours_84_days_pre_sample')

            logging.info(f'adding email df')
            if 'has_email' not in df.columns:
                df = add_has_email(df, lang, self.wmf_con)
                self.df_to_db_col(lang, df, 'has_email')

            logging.info(f'adding num prev_thanks_pre_sample')
            if "num_prev_thanks_pre_sample" not in df.columns:
                df = add_thanks_receiving(df, lang,
                                          start_date=self.onboarding_earliest_active_date, end_date=self.onboarding_latest_active_date,
                                          wmf_con=self.wmf_con, col_label='num_prev_thanks_84_pre_sample')
                self.df_to_db_col(lang, df, 'num_prev_thanks_pre_sample')

            logging.info(f"Group {lang}-{group_name} Saving {len(df)} as included.")
            df['user_included'] = True
            self.df_to_db_col(lang, df, 'user_included')


    def get_quality_data_for_group(self, super_group, lang, group_name, inclusion_criteria=None):
        logging.info(f"Working on group {lang}-{group_name}.")
        group_experience_levels = inclusion_criteria['experience_levels']
        target_user_count = inclusion_criteria['user_count']

        # get the known users so we don't save a candidate twice
        known_users = self.db_session.query(candidates).filter(candidates.lang == lang). \
            filter(candidates.user_experience_level.in_(group_experience_levels)).all()
        logging.info(f"Group {lang}-{group_name} has {len(known_users)} known users.")

        # get the included users to know if we have enough users for this
        included_users_q = self.db_session.query(candidates).filter(candidates.lang == lang). \
            filter(candidates.user_experience_level.in_(group_experience_levels)). \
            filter(candidates.user_included == True)
        included_users = included_users_q.all()
        logging.info(f"Group {lang}-{group_name} has {len(included_users)} included users.")

        # checking if group is done.
        if len(included_users) >= target_user_count:
            logging.info(f"Group {lang}-{group_name} has enough users, nothing to do")
            return pd.read_sql(included_users_q.statement, included_users_q.session.bind)

        # subsetting active users to group criteira
        group_df = super_group[
            super_group['user_experience_level'].apply(
                lambda ue: ue in group_experience_levels)]

        # take a shortcut if it's configured
        if self.max_onboarders_to_check:
            # group_df = group_df[:self.max_onboarders_to_check]
            group_df = group_df.sample(self.max_onboarders_to_check)

        needing_saving = group_df[
            group_df['user_id'].apply(lambda uid: uid not in [u.user_id for u in known_users])]
        self.df_to_db(needing_saving)

        # enqueue jobs
        group_df = self.add_num_quality_df(group_df, lang)
        self.db_session.commit()
        logging.info("raw return from add_num_quality")
        logging.info(f"Group {lang}-{group_name} has {len(group_df)} users with editcount_quality data.")
        logging.info(f"Group df user_editcount_quality head: {group_df['user_editcount_quality'].head(5)}")
        logging.info(f"Group df user_editcount_quality maxitem: {group_df['user_editcount_quality'].max()}")
        logging.info(f"Reminder min edit quality count is: {self.min_edit_count}")
        logging.info(f"Type of  group_df['user_editcount_quality']: {group_df['user_editcount_quality'].dtypes}")
        # small cleaning step
        group_df = group_df.fillna(value={'user_editcount_quality': 0}, downcast='infer')
        # group_df.to_csv(f'add_num_quality.{lang}.{group_name}.csv')
        # sample down to target size and set the inclusion flag
        logging.info("after supposedly filling nan's with 0 for edit count quality")
        logging.info(f"Group {lang}-{group_name} has {len(group_df)} users with editcount_quality data.")
        logging.info(f"Group df user_editcount_quality head: {group_df['user_editcount_quality'].head(5)}")
        logging.info(f"Group df user_editcount_quality maxitem: {group_df['user_editcount_quality'].max()}")
        logging.info(f"Reminder min edit quality count is: {self.min_edit_count}")
        logging.info(f"Type of  group_df['user_editcount_quality']: {group_df['user_editcount_quality'].dtypes}")
        group_min_qual = group_df[group_df['user_editcount_quality'] >= self.min_edit_count]
        logging.info(f"Group {lang}-{group_name} has {len(group_min_qual)} active users min 4 quality edits.")
        if len(group_min_qual) < target_user_count:
            logging.warning(f"Group {lang}-{group_name} has a sampling problem. {len(group_min_qual)} active "
                            f"users min 4 quality edits and target sample size of {target_user_count}")
            target_user_count = len(group_min_qual)
        group_min_qual_incl = group_min_qual.sample(n=target_user_count)

        return group_min_qual_incl


    def add_num_quality_df(self, df, lang):
        """
        get the number of quality users for users in the data frame, in a parallel way using rq
        :param df:
        :return: original dataframe with extra column the number of quality edits this user has in their last max-50 edits
        """

        # check if this is null candidates.user_editcount_quality
        # for those not null, enqueue job that write to database
        # wait for queue to finish
        # make relation and return to df

        user_ids = df[df['lang'] == lang]['user_id'].values
        user_to_job = []
        for user_id in user_ids:
            user_rec = self.db_session.query(candidates).filter(candidates.lang == lang).filter(
                candidates.user_id == user_id).first()
            if user_rec:
                if user_rec.user_editcount_quality is None:
                    user_to_job.append(user_id)

        queue_successufully_ran = False
        while not queue_successufully_ran:
            queue_results = []
            for user_id in user_to_job:
                queue_result = self.q.enqueue(f=add_num_quality_user, args=(user_id, lang, self.config['namespace_fn']))
                queue_results.append({"user_id": user_id, "job": queue_result})

            while not self.q.is_empty():
                logging.debug(f"Queue {self.q.name} still has {self.q.count} jobs left")
                time.sleep(10)
                # check the failed queue and reschedule.

            failed_user_ids = [queue_result['user_id'] for queue_result in queue_results if
                               queue_result['job']._id in self.failed_q.job_ids]
            logging.info(f"Detected failed jobs {failed_user_ids}")
            if not failed_user_ids:
                queue_successufully_ran = True
            else:
                user_to_job = failed_user_ids

        # add num_quality back into the dataframe
        self.db_session.commit()
        num_quality_dfs = []
        for user_id in user_ids:
            num_quality = self.db_session.query(candidates).filter(candidates.lang == lang).filter(
                candidates.user_id == user_id).one().user_editcount_quality
            logging.debug(f'putting data back into num quality is {num_quality} for user {user_id}')
            num_quality = float('nan') if num_quality is None else num_quality
            user_thank_count_df = pd.DataFrame.from_dict({"user_editcount_quality": [num_quality],
                                                          'user_id': [user_id],
                                                          'lang': [lang]}, orient='columns')
            num_quality_dfs.append(user_thank_count_df)

        quality_counts_df = pd.concat(num_quality_dfs)
        df = pd.merge(df, quality_counts_df, how='left', on=['lang', 'user_id'])
        return df

    def df_to_db(self, df):
        for i, row in df.iterrows():
            self.db_session.add(candidates(lang=row['lang'],
                                           user_id=row['user_id'],
                                           user_name=row['user_name'],
                                           user_registration=row['user_registration'],
                                           user_editcount=row['user_editcount'],
                                           user_editcount_quality=None,
                                           user_experience_level=row['user_experience_level'], ))
            self.db_session.commit()

    def df_to_db_col(self, lang, df, col):
        for i, row in df.iterrows():
            cand = self.db_session.query(candidates).filter(candidates.lang == lang).filter(
                candidates.user_id == row['user_id']).one()
            val = row[col]
            setattr(cand, col, val)
            # logging.info(f"value {val} set on {col}, for {cand}")
            self.db_session.add(cand)
        self.db_session.commit()

    def iterative_representative_sampling(self, candidate_inserts):
        """
        - iterating over groups that need more users
        - until reach target group size or out of candidates
        - for randomnly-ordered unincluded user in group
        - get and store edit quality user
        :return: includable-users
        """
        raise NotImplementedError

    def refresh_user_edits_comparative(self, refresh_user, lang):
        """assumption we are only refreshing users who are known to need refresh.
        we assume that another process calculates who needs refersh based on their edit count.
        In additon just doing 1 `lang` at a time. So a calling function would have to loop over langs"""
        # do this in a user-oriented way, or a process-oriented way?
        # revisions of users
        small_user_df = pd.DataFrame({"user_id": [refresh_user.user_id]})
        logging.info(f"starting to get quality edits for user {refresh_user.id}")
        # already received revisions
        already_revs_res = self.db_session.query(edits).filter(edits.lang == lang).filter(
            edits.candidate_id == refresh_user.id).all()
        already_revs = set([r.rev_id for r in already_revs_res])
        logging.info(f"already have {len(already_revs)} revs for user {refresh_user.id}")

        new_user_revs = get_quality_edits_of_users(small_user_df, lang, self.wmf_con, exclusion_rev_ids=already_revs)
        # revisions needing getting = revs - already
        revs_to_get = set(new_user_revs['rev_id'].values)

        # get and store.
        logging.info(f"getting display data for {len(revs_to_get)} revs for user {refresh_user.id}")
        display_data = get_display_data(list(revs_to_get), lang)

        edits_to_add = []
        ets_to_add = []
        for rev_id, display_datum in display_data.items():
            edit_meta = {"lang": lang, "rev_id": rev_id, "candidate_id": refresh_user.id}
            edit = {**edit_meta, **display_datum}
            # from IPython import embed; embed()
            edit_to_add = edits(**edit)
            edits_to_add.append(edit_to_add)

            et_to_add = ExperimentThing(
                            id=f'edit:{lang}:{rev_id}',
                            thing_id=None,
                            experiment_id=-10,
                            randomization_condition=None,
                            randomization_arm=None,
                            object_platform=PlatformType.WIKIPEDIA,
                            object_type=ThingType.WIKIPEDIA_EDIT,
                            object_created_dt=datetime.utcnow(),
                            query_index=f'user:{refresh_user.lang}:{refresh_user.user_id}', #make it easy to lookup the edits of a user later
                            syncable=True,
                            synced_dt=None,
                            metadata_json=edit)

            ets_to_add.append(et_to_add)

        return edits_to_add, ets_to_add

    def refresh_edits(self, lang):
        """
        - update last k edits of included, not completed users
        - determine quality of edits
        - get editDisplay data
        :param user:
        :param lang:
        :return:
        """
        logging.info("starting refresh")
        refresh_users = self.db_session.query(candidates).filter(candidates.lang == lang). \
            filter(candidates.user_included == True). \
            filter(candidates.user_completed == False). \
            all()

        logging.info(f"found {len(refresh_users)} users to refresh for {lang}.")
        for refresh_user in refresh_users:
            user_refresh_data, user_refresh_ets = self.refresh_user_edits_comparative(refresh_user, lang)

            # add our local rows that are *wide*
            self.db_session.add_all(user_refresh_data)
            self.db_session.commit()

            # add in the experiment thing way
            self.db_session.add_all(user_refresh_ets)
            self.db_session.commit()


    def output_population(self):
        # if we've processed every lang
        out_fname = f"all-thankees-historical-{date.today().strftime('%Y%m%d')}.csv"
        out_base = os.path.join(self.config['dirs']['project'], self.config['dirs']['output'])
        if not os.path.exists(out_base):
            os.makedirs(out_base, exist_ok=True)
        out_f = os.path.join(out_base, out_fname)

        # now doing it the sqlalchemy way
        all_included_users = self.db_session.query(candidates).filter(candidates.user_included==True)

        out_df = pd.read_sql(all_included_users.statement, all_included_users.session.bind)
        # now
        out_df = out_df.rename(columns={'user_experience_level': 'prev_experience'})

        logging.info(f"outputted data to: {out_f}")
        out_df.to_csv(out_f, index=False)


    def receive_active_uncompleted_users(self, lang):
        """
        - for users that are in the experiment get from backend whether they still need refreshing
        :return:
        """

    def receive_users_in_thanker_experiment(self, lang):
        """
        - may only be once , but need to know who is in the thanker expirement.
        :return:
        """
        thankers_d = os.path.join(self.config['dirs']['project'], self.config['dirs']['thankers'])
        thankers_ls = os.listdir(thankers_d)
        thankers_ls_lang = [f for f in thankers_ls if f.startswith(lang)]
        try:
            thankers_f = os.path.join(self.config['dirs']['project'], self.config['dirs']['thankers'], thankers_ls_lang[0])
            thankers = pd.read_csv(thankers_f)
            thankers_lang = thankers[thankers['lang']==lang]
            self.users_in_thanker_experiment[lang] = thankers_lang['user_id'].values
            logging.info(f'loaded {len(self.users_in_thanker_experiment[lang])} thankers')
        except IndexError:
            logging.warning(f"No thankers found to load in {self.config['dirs']['thankers']}")

    def run(self, fn):
        # lang loop stage
        for lang in self.langs.keys():
            logging.info(f"working on {lang}")
            if fn == "onboard":
                logging.info(f"receiving thankers")
                self.receive_users_in_thanker_experiment(lang)
                logging.info(f"sampling populations")
                self.sample_population(lang)
            elif fn == "refresh":
                self.refresh_edits(lang)
            elif fn == "sync":
                self.send_included_users_edits_to_cs_hq(lang)
            elif fn == "run":
                self.receive_active_uncompleted_users(lang)
                self.receive_users_in_thanker_experiment(lang)
                self.sample_population(lang)
                self.refresh_edits(lang)
                self.send_included_users_edits_to_cs_hq(lang)
        # final stage
        if fn == 'onboard':
            self.output_population()


@click.command()
@click.option("--fn", default="run", help="the portion to run")
@click.option("--config", default="onboarder_thankee_test.yaml", help="the config file to use")
def run_onboard(fn, config):
    onboarder = thankeeOnboarder(config)
    onboarder.run(fn)


if __name__ == "__main__":
    logging.info("Starting Oboarder")
    run_onboard()
