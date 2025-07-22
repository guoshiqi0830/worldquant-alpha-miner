from sqlalchemy.orm import Session
from sqlalchemy import text


def insert_queue(db: Session, regular, settings, type = 'REGULAR', template_id = 0):
    db.execute(text(f"insert into simulation_queue (regular, settings, type, template_id) \
    values('{regular}', '{settings}', '{type}', '{template_id}' )"))
    db.commit()

def delete_queue_by_template_id(db: Session, template_id):
    db.execute(text(f"delete from simulation_queue where template_id = '{template_id}'"))
    db.commit()

def delete_queue_by_id(db: Session, id):
    db.execute(text(f"delete from simulation_queue where id = {id}"))
    db.commit()
