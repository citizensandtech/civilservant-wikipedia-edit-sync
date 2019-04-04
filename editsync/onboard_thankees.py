import os
import sys
from operator import eq

import click
import pandas as pd
import sqlalchemy
import yaml
import civilservant.logs;
from civilservant.wikipedia.queries.revisions import get_quality_edits_of_users, get_display_data

civilservant.logs.initialize();
import logging
from civilservant.util import init_db_session

from civilservant.wikipedia.connections.database import make_wmf_con
from civilservant.models.wikipedia.thankees import candidates, edits
from civilservant.wikipedia.utils import to_wmftimestamp, from_wmftimestamp, decode_or_nan, add_experience_bin, WIKIPEDIA_START_DATE

# from gratsample.sample_thankees import make_populations, remove_inactive_users, add_experience_bin, add_edits_fn, \
#     remove_with_min_edit_count, add_thanks, add_has_email_currently, add_num_quality, \
#     stratified_subsampler
# from gratsample.sample_thankees_revision_utils import get_recent_edits_alias
# from gratsample.wikipedia_helpers import to_wmftimestamp, make_wmf_con, namespace_all, namespace_mainonly, namespace_nontalk
#
# import os
# import pandas as pd
# from gratsample.cached_df import make_cached_df

from datetime import timedelta, datetime


# def sample_thankees_group_oriented(lang, db_con):
#     """
#         leaving this stub here because eventually i want to first start from what groups need
#         :param lang:
#         :param db_con:
#         :return:
#         """
#     # load target group sizes
#     # figure out which groups need more users
#
#     # sample active users
#     # remove users w/ < n edits
#     # remove editors in
#
# @make_cached_df('active_users')
def get_active_users(lang, start_date, end_date, min_rev_id, wmf_con):
    """
    Return the first and last edits of only active users in `lang`wiki
    between the start_date and end_date.
    """
    wmf_con.execute(f'use {lang}wiki_p;')

    active_sql = """select :lang as lang, user_id, user_name, user_registration, user_editcount
                        from (select distinct(rev_user) from revision 
                            where rev_timestamp >= :start_date and rev_timestamp <= :end_date
                            and rev_id > :min_rev_id) active_users
                        join user on active_users.rev_user=user.user_id;"""
    active_sql_esc = sqlalchemy.text(active_sql)
    params = {"start_date": int(to_wmftimestamp(start_date)),
              "end_date": int(to_wmftimestamp(end_date)),
              "min_rev_id": min_rev_id,
              "lang":lang}
    active_df = pd.read_sql(active_sql_esc, con=wmf_con, params=params)
    active_df['user_registration'] = active_df['user_registration'].apply(from_wmftimestamp, default=WIKIPEDIA_START_DATE)
    active_df['user_name'] = active_df['user_name'].apply(decode_or_nan)
    return active_df


#
#
#
# def make_data(subsample, wikipedia_start_date, sim_treatment_date, sim_observation_start_date, sim_experiment_end_date,
#               wmf_con):
#     print('starting to make data')
#     df = make_populations(start_date=wikipedia_start_date, end_date=sim_treatment_date, wmf_con=wmf_con)
#     df = remove_inactive_users(df, start_date=sim_observation_start_date, end_date=sim_treatment_date)
#     if not subsample:
#         output_bin_stats(df)
#     df = add_experience_bin(df)
#     print('Simulated Active Editors')
#     print(df.groupby(['lang','experience_level_pre_treatment']).size())
#     if subsample:
#         print(f'make a first reasonable subsample of {10*subsample} samples per group to be able to get their edit counts beforehand')
#         print('this wouldnt be as big of a problem live because edit count is easy to get live')
#         df = stratified_subsampler(df, 10*subsample, newcomer_multiplier=5)
#
#     print('Random Stratified Subsample of active Editors to get edit counts with last 90')
#     print(df.groupby(['lang','experience_level_pre_treatment']).size())
#
#     df = add_edits_fn(df, col_name='recent_edits_pre_treatment', timestamp_list_fn=len, edit_getter_fn=get_recent_edits_alias, wmf_con=wmf_con)
#     df = remove_with_min_edit_count(df, min_edit_count=4) #  in the future remove this step by just including edit_count from the make_populations step
#     print('Random Stratified Subsample Having min 4 edits in the last 90')
#     print(df.groupby(['lang','experience_level_pre_treatment']).size())
#     if subsample:
#         print(f'subsetting to {subsample} samples')
#         df = stratified_subsampler(df, subsample)
#
#     print('Second Random Stratified subsample to Get Edit Quality Data')
#     print(df.groupby(['lang','experience_level_pre_treatment']).size())
#
#     print("adding thanks")
#     df = add_thanks(df, start_date=sim_observation_start_date, end_date=sim_treatment_date,
#                     col_name='num_prev_thanks_in_90_pre_treatment', wmf_con=wmf_con)
#
#     print("adding email")
#     df = add_has_email_currently(df, wmf_con=wmf_con)
#
#     print("adding quality")
#     df = add_num_quality(df, col_name='num_quality_pre_treatment', wmf_con=wmf_con, namespace_fn=namespace_all, end_date=sim_treatment_date)
#     print("adding quality nontalk")
#     df = add_num_quality(df, col_name='num_quality_pre_treatment_non_talk', namespace_fn=namespace_nontalk, end_date=sim_treatment_date, wmf_con=wmf_con)
#     print("adding quality main only")
#     df = add_num_quality(df, col_name='num_quality_pre_treatment_main_only', namespace_fn=namespace_mainonly, end_date=sim_treatment_date, wmf_con=wmf_con)
#
#     print('done')
#     return df

class thankeeOnboarder():
    def __init__(self, config_file):
        """groups needing edits and size N edits to be included which k edits to be displayed
        """
        config = yaml.safe_load(open(os.path.join('config', config_file), 'r'))
        self.groups = config['groups']
        self.langs = config['langs']
        self.wmf_con = make_wmf_con()
        self.db_session = init_db_session()
        self.experiment_start_date = config['experiment_start_date']
        self.onboarding_earliest_active_date = self.experiment_start_date - timedelta(days=90)
        self.onboarding_latest_active_date = datetime.utcnow()
        self.populations = {}

    def add_update_candidate_users(self, knowns, candidates_df):
        candidates_inserts = []
        candidates_updates = []

        # wanted to use database style joins, but i'll just use looping algos for now.
        # knowns_user_ids = [c.user_id for c in knowns]
        knowns_objs_dict = {c.user_id: c for c in knowns}

        for row in candidates_df.iterrows():
            cand = row[1].to_dict()
            cand_user_id = cand['user_id']
            # if the new candidates user id isn't in the knowns, we need to insert it
            if not cand_user_id in knowns_objs_dict.keys():
                candidates_inserts.append(candidates(**cand))
            # if the candidate is known, they might have a new edit count
            else:
                known = knowns_objs_dict[cand_user_id]
                # if the new edit count is higher
                if cand['user_editcount'] > known.user_editcount:
                    known.user_editcount = cand['user_editcount']
                    candidates_updates.append(known)

        return candidates_inserts, candidates_updates

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
        """
        known_users = self.db_session.query(candidates).filter(candidates.lang == lang).all()

        if known_users:
            return

        active_users = get_active_users(lang, start_date=self.onboarding_earliest_active_date,
                                        end_date=self.onboarding_latest_active_date,
                                        min_rev_id=self.langs[lang]['min_rev_id'],
                                        wmf_con=self.wmf_con)


        active_users_min_edits = active_users[active_users['user_editcount']>=4]
        active_users_min_edits_nonthanker = active_users_min_edits[active_users_min_edits["user_id"].apply(lambda uid: uid not in self.users_in_thanker_experiment[lang])]
        active_users_min_edits_nonthanker_exp = add_experience_bin(active_users_min_edits_nonthanker, self.experiment_start_date)

        candidate_inserts, candidate_updates = self.add_update_candidate_users(known_users, active_users_min_edits_nonthanker_exp)
        logging.info(f'candidate inserts length {len(candidate_inserts)}')
        if candidate_inserts:
            candidate_inserts_includable = self.iterative_representative_sampling(candidate_inserts)
            self.db_session.add_all(candidate_inserts_includable)
            self.db_session.commit()



    def iterative_representative_sampling(self, candidate_inserts):
        """
        - iterating over groups that need more users
        - until reach target group size or out of candidates
        - for randomnly-ordered unincluded user in group
        - get and store edit quality user
        :return: includable-users
        """
        #TODO this is just a shortcut for now.
        candidate_inserts_includable = candidate_inserts[:100]
        for cand in candidate_inserts_includable:
            cand.user_included = True
        return candidate_inserts_includable


    def refresh_user_edits_comparative(self, refresh_user, lang):
        """assumption we are only refreshing users who are known to need refresh.
        we assume that another process calculates who needs refersh based on their edit count.
        In additon just doing 1 `lang` at a time. So a calling function would have to loop over langs"""
        # do this in a user-oriented way, or a process-oriented way?
        # revisions of users
        small_user_df = pd.DataFrame({"user_id":[refresh_user.user_id]})
        all_user_revs = get_quality_edits_of_users(small_user_df, lang, self.wmf_con)
        # already received revisions
        already_revs_res = self.db_session.query(edits).filter(edits.lang==lang).filter(edits.candidate_id==refresh_user.candidate_id).all()
        # revisions needing getting = revs - already
        already_revs= set([r.rev_id for r in already_revs_res])
        live_revs = set(all_user_revs['rev_id'].values)
        revs_to_get = live_revs.difference(already_revs)

        # get and store.
        display_data = get_display_data(list(revs_to_get), lang)

        new_revs = edits(**display_data)
        return new_revs


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
        refresh_users = self.db_session.query(candidates).filter(candidates.lang==lang).\
                                                          filter(candidates.user_included==True).\
                                                          filter(candidates.user_completed==False).\
                                                          all()

        logging.info(f"found {len(refresh_users)} users to refresh for {lang}.")
        refresh_data = []
        for refresh_user in refresh_users:
            user_refresh_data = self.refresh_user_edits_comparative(refresh_user, lang)
            refresh_data.append(user_refresh_data)

        self.db_session.add_all(refresh_data)
        self.db_session.commit()



    def send_included_users_edits_to_cs_hq(self, lang):
        """
        - send newly included users to cs hq
        - send new editdisplay data back to cs hq
        - over api
        :return:
        """

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
        #TODO stopgap measure
        self.users_in_thanker_experiment = {"ar":[], "de":[], "fa":[], "pl":[]}

    def run(self, fn):
        for lang in self.langs.keys():
            if fn == "onboard":
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


@click.command()
@click.option("--fn", default="run", help="the portion to run")
def run_onboard(fn):
    config_file = os.getenv('ONBOARDER_CONFIG', 'onboarder.yaml')
    onboarder = thankeeOnboarder(config_file)
    onboarder.run(fn)

if __name__ == "__main__":
    logging.info("Starting Oboarder")
    run_onboard()
