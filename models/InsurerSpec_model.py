from database.exts import db
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship, backref


class InsurerSpec(db.Model):
    __tablename__ = 'insurerspeccontentbiz'
    numCode = db.Column(db.String(50), primary_key=True)
    insurerName = db.Column(db.String(50))
    insurerType = db.Column(db.String(50))
    specContent = db.Column(db.String(50))
    remark = db.Column(db.String(50))

    def __init__(self):
        self.guid = None

    def save(self):
        db.session.add(self)
        db.session.commit()

    def delete(self):
        db.session.delete(self)
        db.session.commit()

    def update(self):
        db.session.commit()

    def to_json(self):
        dict = self.__dict__
        if "_sa_instance_state" in dict:
            del dict["_sa_instance_state"]
        return dict