from civilservant.models.wikipedia.thankees import candidates
from civilservant.util import init_db_session
from civilservant.wikipedia.connections.database import make_wmf_con
from civilservant.wikipedia.queries.revisions import num_quality_revisions
import civilservant.logs
# civilservant.logs.initialize()
# import logging
from civilservant.wikipedia.utils import get_namespace_fn


def add_num_quality_user(user_id, lang, namespace_fn_name):
    db_session = init_db_session()
    wmf_con = make_wmf_con()
    namespace_fn = get_namespace_fn(namespace_fn_name)
    num_quality = num_quality_revisions(user_id=user_id, lang=lang, wmf_con=wmf_con,
                                        namespace_fn=namespace_fn)
    user_rec = db_session.query(candidates).filter(candidates.lang == lang).filter(
            candidates.user_id == user_id).first()
    user_rec.user_editcount_quality = num_quality
    db_session.add(user_rec)
    db_session.commit()
