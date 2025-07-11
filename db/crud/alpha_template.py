from sqlalchemy.orm import Session
from sqlalchemy import text


def upsert_alpha_template(db: Session, alpha_id, template, params ):
    result = db.execute(text(f"select id from alpha_template where alpha_id = '{alpha_id}'"))
    if result.first():
        db.execute(text(f"update alpha_template set template = '{template}', params = '{params}' where alpha_id = '{alpha_id}'"))
    else:
        db.execute(text(f"insert into alpha_template (alpha_id, template, params) \
        values('{alpha_id}', '{template}', '{params}' )"))
    db.commit()
