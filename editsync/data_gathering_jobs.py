import pandas as pd

from civilservant.db import init_session
import civilservant.models.core
from civilservant.models.wikipedia.thankees import candidates
from civilservant.wikipedia.connections.database import make_wmf_con
from civilservant.wikipedia.queries.revisions import num_quality_revisions, get_timestamps_within_range
import civilservant.logs
# civilservant.logs.initialize()
# import logging
from civilservant.wikipedia.utils import get_namespace_fn, calc_labour_hours

from civilservant.wikipedia.queries.user_interactions import get_user_disablemail_properties, get_thanks_receiving


def add_num_quality_user(user_id, lang, namespace_fn_name):
    db_session = init_session()
    wmf_con = make_wmf_con()
    namespace_fn = get_namespace_fn(namespace_fn_name)
    num_quality = num_quality_revisions(user_id=user_id, lang=lang, wmf_con=wmf_con,
                                        namespace_fn=namespace_fn)
    user_rec = db_session.query(candidates).filter(candidates.lang == lang).filter(
        candidates.user_id == user_id).first()
    user_rec.user_editcount_quality = num_quality
    db_session.add(user_rec)
    db_session.commit()


def add_has_email(df, lang, wmf_con):
    user_prop_dfs = []
    user_ids = df['user_id'].values
    for user_id in user_ids:
        user_prop_df = get_user_disablemail_properties(lang, user_id, wmf_con)
        has_email = False if len(
            user_prop_df) >= 1 else True  # the property disables email, if it doesn't exist the default its that it's on
        user_prop_dfs.append(pd.DataFrame.from_dict({'has_email': [has_email],
                                                     'user_id': [user_id],
                                                     'lang': [lang]}, orient='columns'))

    users_prop_df = pd.concat(user_prop_dfs)
    df = pd.merge(df, users_prop_df, how='left', on=['user_id', 'lang'])
    return df


def add_thanks_receiving(df, lang, start_date, end_date, wmf_con, col_label):
    user_thank_count_dfs = []
    user_names = df['user_name'].values
    for user_name in user_names:
        user_thank_df = get_thanks_receiving(lang, user_name,
                                           start_date=start_date,
                                           end_date=end_date,
                                           wmf_con=wmf_con)
        user_thank_count_df = pd.DataFrame.from_dict({col_label: [len(user_thank_df)],
                                                      'user_name': [user_name],
                                                      'lang': [lang]}, orient='columns')
        user_thank_count_dfs.append(user_thank_count_df)

    thank_counts_df = pd.concat(user_thank_count_dfs)
    df = pd.merge(df, thank_counts_df, how='left', on=['user_name', 'lang'])
    return df


def add_labour_hours(df, lang, col_label, wmf_con, start_date, end_date):
    """ get the labour hour of a users in a df and lang between start and end dates"""
    return add_edits_fn(df=df, lang=lang, col_label=col_label, wmf_con=wmf_con,
                        timestamp_list_fn=calc_labour_hours, edit_getter_fn=get_timestamps_within_range,
                        start_date=start_date, end_date=end_date)

def add_edits_fn(df, lang, col_label, wmf_con, timestamp_list_fn, edit_getter_fn, start_date=None, end_date=None):
    '''add the number of edits a user made within range'''
    edit_measure_dfs =[]
    user_ids = df[df['lang'] == lang]['user_id'].values
    for user_id in user_ids:
        ts_series = edit_getter_fn(lang, user_id, wmf_con, start_date, end_date)
        ts_list = list(ts_series['rev_timestamp'])
        edit_measure_df = pd.DataFrame.from_dict({col_label: [timestamp_list_fn(ts_list)],
                                                      'user_id': [user_id],
                                                      'lang': [lang]}, orient='columns')
        edit_measure_dfs.append(edit_measure_df)

    edit_measures_df = pd.concat(edit_measure_dfs)
    df = pd.merge(df, edit_measures_df, how='left', on=['lang', 'user_id'])

    return df
