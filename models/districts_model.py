from database.exts import db
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship, backref


class districts(db.Model):
    __tablename__ = 'districts'
    guid = db.Column(db.String, primary_key=True)
    DistrictName = db.Column(db.String(50), nullable=False)
    HeadGUID = db.Column(db.String(50),nullable=False)
    DistrictCode = db.Column(db.String(6))
    DistrictLevelID = db.Column(db.String(1))
    DistrictLevelName = db.Column(db.String(2))
    LicenceCode = db.Column(db.String(10))
    PostCode = db.Column(db.String(6))
    Duplicate = db.Column(db.String(1))
    Pingyin = db.Column(db.String(50))
    DisplayName = db.Column(db.String(50))
    ZACode = db.Column(db.String(50))
    DistrictSimple = db.Column(db.String(50))
    DisplayOrder = db.Column(db.Int(11))
    cutData = db.Column(db.String(1))
    modifyContent = db.Column(db.String(50))
    modifyRela = db.Column(db.String(50))
    PACode = db.Column(db.String(50))
    YGCode = db.Column(db.String(50))
    DHCode = db.Column(db.String(50))
    StandardDisplayName = db.Column(db.String(100))
    longitude = db.Column(db.String(50))
    latitude = db.Column(db.String(50))
    Shortname = db.Column(db.String(50))
    ZLCode = db.Column(db.String(50))
    ZHCode = db.Column(db.String(50))
    HTCode = db.Column(db.String(50))
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
