import json
import logging
import os
from unittest import mock
from unittest.mock import patch
import pytest
import pandas as pd
from civilservant.models.wikipedia.thankees import candidates
from civilservant.util import init_db_session
from civilservant.wikipedia.queries.revisions import get_active_users
from editsync.onboard_thankees import thankeeOnboarder

def load_path_files_to_dict(sub_dirname, filetype):
    sub_dir = os.path.join('test_data', sub_dirname)
    reader_fn = {'.json': lambda f: json.load(open(os.path.join(sub_dir, f), 'r')),
               '.csv': lambda f: pd.read_csv(open(os.path.join(sub_dir, f), 'r'), parse_dates=[4], infer_datetime_format=True)}
    reader = reader_fn[filetype]
    fname_file = {f: reader(f) for f in os.listdir(sub_dir) if f.endswith(filetype)}
    return fname_file

@pytest.fixture
def display_data():
    return load_path_files_to_dict('display_data','.json')

@pytest.fixture
def mwapi_responses():
    return load_path_files_to_dict('mwapi_responses','.json')

@pytest.fixture
def oresapi_responses():
    return load_path_files_to_dict('ores_api_responses', '.json')

@pytest.fixture
def wmf_con_responses():
    return load_path_files_to_dict('con_responses','.csv')

@pytest.fixture
def active_users_responses():
    return load_path_files_to_dict('active_users','.csv')

@pytest.fixture
def add_num_quality_responses():
    return load_path_files_to_dict('add_num_quality','.csv')

@pytest.fixture
def db_session():
    return init_db_session()


def test_onboarder_config():
    onboarder = thankeeOnboarder('onboarder_thankee_test.yaml')
    assert onboarder.max_onboarders_to_check == 10

def clear_cands(db_session):
    db_session.query(candidates).delete()


@patch('editsync.onboard_thankees.thankeeOnboarder.add_num_quality_df')
@patch('civilservant.wikipedia.queries.revisions.get_active_users')
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
    # included_dfs = []
    # for lang, group in onboarder.populations.items():
    #     for group_name, group_df in group.items():
    #         included_dfs.append(group_df)
    # included_count = sum([len(df) for df in included_dfs])
    candidates_count = db_session.query(candidates).count()
    included_count = db_session.query(candidates).filter(candidates.user_included==True).count()
    # assert len(cands) == len(active_users_responses['active_users.ar.csv']) + len(active_users_responses['active_users.de.csv'])
    assert candidates_count == onboarder.max_onboarders_to_check*len(add_num_quality_responses)
    assert included_count == onboarder.config['langs']['ar']['groups']['newcomer']['user_count'] + \
                               onboarder.config['langs']['ar']['groups']['experienced']['user_count']+ \
                               onboarder.config['langs']['de']['groups']['newcomer']['user_count'] + \
                               onboarder.config['langs']['de']['groups']['experienced']['user_count']
