select substring(username, 1, 2) as lang, substring(username, 4) as user_name, modified_dt, created_dt
  from core_oauth_users
  where created_dt <= date(20191029);
