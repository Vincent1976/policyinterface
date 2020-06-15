from database.exts import db
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship, backref


class GJXXPT_Product(db.Model):
    _tablename__ = 'GJXXPT_Product'
    GUID = db.Column(db.String, primary_key=True)
    appkey = db.Column(db.String(50), nullable=False)
    InsuranceCoverageCode = db.Column(db.String(50))
    numCode = db.Column(db.String(50))
    InsuranceCode = db.Column(db.String(50))
    InsuranceCoverageName = db.Column(db.String(50))
    ChargeTypeCode = db.Column(db.String(50))
    Rate = db.Column(db.String(50))
    deductible = db.Column(db.String(50))
    MonetaryAmount = db.Column(db.String(50))
    CargoTypeClassification1 = db.Column(db.String(50))
    Remark = db.Column(db.String(50))
    TransportModeCode = db.Column(db.String(50))
    InsurerCode = db.Column(db.String(50))
    SendMode = db.Column(db.String(50))
    Rate2 = db.Column(db.String(50))
    PolicyAmount = db.Column(db.Decimal(18,2))
    BXcargoCode = db.Column(db.String(50))
    BXcargoName = db.Column(db.String(50))
    CustCode = db.Column(db.String(50))
    PolicyNo = db.Column(db.String(50))
    CustName = db.Column(db.String(50))

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