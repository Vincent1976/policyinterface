from database.exts import db
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship, backref


class ValidInsured(db.Model):
    __tablename__ = 'validinsured'
    GUID = db.Column(db.String, primary_key=True)
    Appkey = db.Column(db.String(50), nullable=False)
    ValidInsuredName = db.Column(db.String(50))
    UniCode = db.Column(db.String(50))
    PolicyNo = db.Column(db.String(50))
    ClntMrk = db.Column(db.String(50))
    CertfCls = db.Column(db.String(50))
    CertfCde = db.Column(db.String(50))
    Province = db.Column(db.String(50))
    City = db.Column(db.String(50))
    Tel = db.Column(db.String(50))
    EstablishTm = db.Column(db.String(50))
    InvoiceTitle = db.Column(db.String(50))
    CertfClsName = db.Column(db.String(50))
    DetailAddress = db.Column(db.String(50))

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