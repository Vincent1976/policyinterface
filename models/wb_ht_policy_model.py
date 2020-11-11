from database.exts import db
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship, backref


class wb_ht_remotedata(db.Model):
    __tablename__ = 'wb_ht_remotedata'
    guid = db.Column(db.String, primary_key=True)
    appkey = db.Column(db.String(50), nullable=False)
    custId = db.Column(db.String(50))
    custCoName = db.Column(db.String(200))
    custUserName = db.Column(db.String(50))
    custEmail = db.Column(db.String(200))
    custPhone = db.Column(db.String(50))
    shipperId = db.Column(db.String(50))
    shipperName = db.Column(db.String(200))
    insuredName = db.Column(db.String(200))
    channelOrderId = db.Column(db.String(50))
    bizContent = db.Column(db.String(50))
    termContent = db.Column(db.String(50))
    deliveryOrderId = db.Column(db.String(500))
    shipId = db.Column(db.String(50))
    licenseId = db.Column(db.String(500))
    cargoFreight = db.Column(db.String(50))
    claimLimit = db.Column(db.String(50))
    cargeValue = db.Column(db.String(50))
    invoicePrice = db.Column(db.String(10))
    includeTax = db.Column(db.String(10))
    insuranceFee = db.Column(db.String(50))
    trafficType = db.Column(db.String(10))
    shippingType = db.Column(db.String(20))
    lotType = db.Column(db.String(10))
    packageType = db.Column(db.String(500))
    cargoType = db.Column(db.String(50))
    cargoName = db.Column(db.String(4000))
    departProvince = db.Column(db.String(20))
    departCity = db.Column(db.String(100))
    departDistrict = db.Column(db.String(20))
    destinationProvice = db.Column(db.String(20))
    destinationCity = db.Column(db.String(100))
    destinationDistrict = db.Column(db.String(20))
    consigneeName = db.Column(db.String(200))
    consigneePhone = db.Column(db.String(50))
    departDateTime = db.Column(db.String(50))
    cargoCount = db.Column(db.String(50))
    cargoWeight = db.Column(db.String(50))
    stealFlag = db.Column(db.String(10))
    CreateDate = db.Column(db.String(50))
    policyNo = db.Column(db.String(50))
    deliveryAddress = db.Column(db.String(500))
    volume = db.Column(db.String(50))
    relationType = db.Column(db.String(500))
    departSpot = db.Column(db.String(500))
    transitSpot = db.Column(db.String(500))
    arriveStation = db.Column(db.String(500))
    systemOrderId = db.Column(db.String(50))
    custProperty = db.Column(db.String(50))
    shipperProperty = db.Column(db.String(50))
    departStation = db.Column(db.String(500))
    Status = db.Column(db.String(50))
    errLog = db.Column(db.String(500))
    FeedbackStatus = db.Column(db.String(50))
    ExceptionStatus = db.Column(db.String(50))
    shipperContact = db.Column(db.String(50))
    arriveStreet = db.Column(db.String(500))
    arriveProperty = db.Column(db.String(50))
    deliveryContact = db.Column(db.String(50))
    alarmEvent = db.Column(db.String(50))
    departStreet = db.Column(db.String(50))
    cargoKind = db.Column(db.String(50))
    invoiceTitle = db.Column(db.String(100))
    mpAmount = db.Column(db.String(50))
    mpRate = db.Column(db.String(50))
    mpObject = db.Column(db.String(50))
    mpRelation = db.Column(db.String(50))
    policyRate = db.Column(db.String(50))
    policySolutionID = db.Column(db.String(50))
    claimLimit2 = db.Column(db.String(50))
    AdjustId = db.Column(db.String(50))

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
