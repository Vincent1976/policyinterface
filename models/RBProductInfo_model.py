from database.exts import db
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship, backref


class RBProductInfo(db.Model):
    _tablename__ = 'RBProductInfo'
    line_no = db.Column(db.String, primary_key=True)
    Insurance = db.Column(db.String(50))
    MainGlauses = db.Column(db.String(50))
    MainGlausesCode = db.Column(db.String(50))
    AdditiveNo = db.Column(db.String(50))
    Additive = db.Column(db.String(50))
    GoodsTypeNo = db.Column(db.String(50))
    SpecialAnnounce = db.Column(db.String(50))
    Deductible = db.Column(db.String(50))
    OtherInfo = db.Column(db.String(50))
    UserName = db.Column(db.String(50))
    DeductiblePercent = db.Column(db.String(50))
    DeductibleAmount = db.Column(db.String(50))

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