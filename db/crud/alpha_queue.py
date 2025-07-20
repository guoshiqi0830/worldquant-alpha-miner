from sqlalchemy.orm import Session
from sqlalchemy import text


def insert_alpha_queue(db: Session, regular, settings, type = 'REGULAR', template_id = 0, template = None, params = None ):
    db.execute(text(f"insert into alpha_queue (regular, settings, type, template_id, template, params) \
    values('{regular}', '{settings}', '{type}', '{template_id}', '{template}', '{params}' )"))
    db.commit()

def delete_alpha_queue_by_template_id(db: Session, template_id):
    db.execute(text(f"delete from alpha_queue where template_id = '{template_id}'"))
    db.commit()

def delete_alpha_queue_by_id(db: Session, id):
    db.execute(text(f"delete from alpha_queue where id = {id}"))
    db.commit()
