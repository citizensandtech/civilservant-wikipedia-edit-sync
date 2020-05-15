import json
import logging
import os
from unittest.mock import patch
import pytest
import pandas as pd
from civilservant.models.wikipedia.thankees import candidates
from civilservant.db import init_session
import sys
sys.path.append('..')
from editsync.onboard_thankees import thankeeOnboarder

def load_path_files_to_dict(sub_dirname, filetype):
    sub_dir = os.path.join('test_data', sub_dirname)
    reader_fn = {'.json': lambda f: json.load(open(os.path.join(sub_dir, f), 'r')),
               '.csv': lambda f: pd.read_csv(open(os.path.join(sub_dir, f), 'r'), parse_dates=[4], infer_datetime_format=True)}
    reader = reader_fn[filetype]
    fname_file = {f: reader(f) for f in os.listdir(sub_dir) if f.endswith(filetype)}
    return fname_file

@pytest.fixture
def active_users_responses():
    return load_path_files_to_dict('active_users','.csv')

@pytest.fixture
def add_num_quality_responses():
    return load_path_files_to_dict('add_num_quality','.csv')

@pytest.fixture
def db_session():
    return init_session()


def clear_cands(db_session):
    db_session.query(candidates).delete()


def test_onboarder_config():
    onboarder = thankeeOnboarder('onboarder_thankee_test.yaml')
    assert onboarder.max_onboarders_to_check == 10


@patch('editsync.onboard_thankees.thankeeOnboarder.add_num_quality_df')
@patch('civilservant.wikipedia.queries.users.get_active_users')
def test_onboarder_sample(mock_rev_utils, mock_add_num, db_session, active_users_responses, add_num_quality_responses):
    clear_cands(db_session)
    mock_rev_utils.side_effect = (active_users_responses['active_users.ar.csv'],
                                  active_users_responses['active_users.de.csv'])
    mock_add_num.side_effect = [add_num_quality_responses['add_num_quality.ar.newcomer.csv'],
                                add_num_quality_responses['add_num_quality.ar.experienced.csv'],
                                add_num_quality_responses['add_num_quality.de.newcomer.csv'],
                                add_num_quality_responses['add_num_quality.de.experienced.csv'],]
    onboarder = thankeeOnboarder('onboarder_thankee_test.yaml', mock_rev_utils, db_session)
    onboarder.run('onboard')
    candidates_count = db_session.query(candidates).count()
    included_count = db_session.query(candidates).filter(candidates.user_included==True).count()

    assert candidates_count == onboarder.max_onboarders_to_check*len(add_num_quality_responses)


@patch('civilservant.wikipedia.queries.revisions.num_quality_revisions')
def test_add_num_quality(mock_num_quality, db_session):
    clear_cands(db_session)
    user_id = 191919
    lang = 'zz'
    example_qual_val = 99
    mock_num_quality.side_effect = [example_qual_val]
    a_cand = candidates(user_id=user_id, lang=lang)
    db_session.add(a_cand)
    db_session.commit()

    from editsync.data_gathering_jobs import add_num_quality_user

    add_num_quality_user(user_id, lang, 'all', mock_num_quality)

    a_cand_qual_val = db_session.query(candidates).filter_by(user_id=user_id, lang=lang).one().user_editcount_quality
    assert example_qual_val == a_cand_qual_val

