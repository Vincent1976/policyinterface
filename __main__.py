# -*- coding: utf-8 -*-
import datetime
import json
import smtplib
import uuid
from email.header import Header
from email.mime.text import MIMEText
from flask import Flask, redirect, render_template, request, send_file, send_from_directory
import commfunc
import config
from database.exts import db
import traceback
import suds
from suds.client import Client
import hashlib
import datetime
import decimal
import logging
import time
import pymysql
import os


# 创建flask对象(测试同步推送)
app = Flask(__name__)
appowner = 'Draginins'  # 软件所有者

# 数据库初始化
app.config.from_object(config)
app.debug = True
db.init_app(app)

# 自定义异常
class MyException(Exception):
    def __init__(self, ErrorInfo):
        super().__init__(self)  # 初始化父类
        self.errorinfo = ErrorInfo

    def __str__(self):
        return self.errorinfo


# 首页路由
@app.route('/', methods=['GET'])
def index():
    return 'Welcome!'

# 投保接口 (通用)
@app.route('/sendpolicy', methods=['POST'])
def sendpolicy():
    from dals import dal
    from models import policy_model
    from models import GJXXPT_Product_model
    from models import districts_model
    try:
        # 获取请求 
        postdata = json.loads(request.get_data(as_text=True))
        policymodel = policy_model.remotedata()
     # 保存客户运单
        policymodel.guid = str(uuid.uuid1())
        policymodel.channelOrderId = postdata['sequencecode']
        policymodel.Status = postdata["status"]
        policymodel.CreateDate = datetime.datetime.now()
        # policymodel.channelOrderId = newPolicyNo
        # policymodel.systemOrderId = policymodel['sequencecode']
        policymodel.appkey = postdata['appkey']            
        policymodel.bizContent = postdata['usercode']
        policymodel.policyNo = postdata['solutionid']
        policymodel.claimLimit = postdata['productid']
        policymodel.custProperty = postdata['applicanttype']
        policymodel.custId = postdata['applicantidnumber']
        policymodel.insuredName = postdata['insuredname']
        policymodel.shipperProperty = postdata['insuredtype']
        policymodel.shipperId = postdata['insuredidnumber']
        policymodel.shipperContact = postdata['spname']
        policymodel.cargeValue = postdata['policyamount']
        policymodel.policyRate = postdata['rate']
        policymodel.termContent = postdata['deductible']
        policymodel.insuranceFee = postdata['premium']
        policymodel.mpObject = postdata['insurancecoveragename']
        policymodel.mpRelation = postdata['chargetypecode']
        policymodel.departDateTime = postdata['insuredatetime']
        policymodel.transitSpot = postdata['originaldocumentnumber']
        policymodel.trafficType = postdata['transportmodecode']
        policymodel.licenseId = postdata['vehiclenumber']
        policymodel.departProvince = postdata['startprovince']
        policymodel.departCity = postdata['startcity']
        policymodel.destinationProvice = postdata['endprovince']
        policymodel.destinationCity = postdata['endcity']
        policymodel.destinationDistrict = postdata['enddistrict']
        policymodel.departSpot = postdata['startaddress']
        policymodel.deliveryAddress = postdata['endaddress']
        policymodel.departStation = postdata['startareacode']
        policymodel.arriveStation = postdata['endareacode']
        policymodel.arriveProperty = postdata['transitaddress']
        policymodel.cargoName = postdata['descriptionofgoods']
        policymodel.cargoType = postdata['cargotype']
        policymodel.packageType = postdata['packagetype']
        policymodel.cargoCount = postdata['packagequantity']
        policymodel.cargoKind = postdata['packageunit']
        policymodel.cargoWeight = postdata['weight']
        policymodel.mpAmount = postdata['weightunit']
        policymodel.volume = postdata['volume']
        policymodel.mpRate = postdata['volumeunit']
        #必填项校验
        exMessage = ''
        
        if policymodel.appkey == "":
            exMessage += "appkey不能为空;"
        if policymodel.bizContent == "":
            exMessage += "usercode不能为空;"
        if policymodel.policyNo == "":
            exMessage += "solutionid不能为空;"
        if policymodel.channelOrderId == "":
            exMessage += "sequencecode不能为空;"
        if policymodel.claimLimit == "":
            exMessage += "productid不能为空;"
        if policymodel.custProperty == "":
            exMessage += "applicanttype不能为空;"
        if policymodel.custId == "":
            exMessage += "applicantidnumber不能为空;"
        if policymodel.insuredName == "":
            exMessage += "insuredName不能为空;"
        if policymodel.shipperProperty == "":
            exMessage += "insuredtype不能为空;"
        if policymodel.shipperId == "":
            exMessage += "insuredidnumber不能为空;"
        if policymodel.cargeValue == "":
            exMessage += "policyamount不能为空;"
        if policymodel.policyRate == "":
            exMessage += "rate不能为空;"
        if policymodel.termContent == "":
            exMessage += "deductible不能为空;"
        if policymodel.insuranceFee == "":
            exMessage += "premium不能为空;"
        if policymodel.mpObject == "":
            exMessage += "insurancecoveragename不能为空;"
        if policymodel.mpRelation == "":
            exMessage += "chargetypecode不能为空;"
        if policymodel.departDateTime == "":
            exMessage += "insuredatetime不能为空;"
        else:        
            if len(policymodel.departDateTime) != 14:
                raise Exception("错误：起运日期departDateTime格式有误，正确格式：20170526153733;")
            else:                   
                # 倒签单校验
                # print(datetime.datetime.now().strftime("%Y%m%d%H%M%S"))
                # 获取当前时间多加一天 并转换为20200617104122(14位)
                # print((datetime.datetime.now()+datetime.timedelta(hours=1)).strftime("%Y%m%d%H%M%S"))
                # minutes = (policymodel.departDateTime - (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
                # m1 = policymodel.departDateTime
                # m2 = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                # print((datetime.datetime.now()+datetime.timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S"))
                # if datetime.datetime.strptime(policymodel.departDateTime).datetime.timedelta(hours=1).Subtract(datetime.datetime.Now()).TotalMinutes < 0:
                #     exMessage += "当前不允许倒签单;"
                departDateTimes = policymodel.departDateTime 
                # policymodel.departDateTime = int(departDateTimes[0:4]) + "-" + int(departDateTimes[4:6]) + "-" + int(departDateTimes[6:8]) + " "+ int(departDateTimes[8:10]) + ":" + int(departDateTimes[10:12])+ ":" + int(departDateTimes[12:14])+"."+"000000"
                print(policymodel.departDateTime)
                time = int(departDateTimes)+10000 # 加10000相当于一个小时
                now = int(datetime.datetime.now().strftime("%Y%m%d%H%M%S"))
                if time - now < 100: # 100相当于小于1分钟
                    exMessage += "当前不允许倒签单;"
                
        if policymodel.transitSpot == "" and policymodel.vehiclenumber =="":
            exMessage += "运单号或者车牌号至少一个必填;"
        if policymodel.trafficType == "":
            exMessage += "transportmodecode不能为空;"
        if policymodel.departSpot == "":
            exMessage += "startaddress不能为空;"
        if policymodel.deliveryAddress == "":
            exMessage += "endaddress不能为空;"
        if policymodel.cargoName == "":
            exMessage += "descriptionofgoods不能为空;"
        if policymodel.cargoType == "":
            exMessage += "cargotype不能为空;"
        if policymodel.cargoCount == "":
            exMessage += "packagequantity不能为空;"

        isSDS = True
        isDQM = False
        # if policymodel.departProvince != "" and policymodel.destinationProvice != "":
        #     a=9
        #三段模式，需校验  
        if policymodel.departStation != "" and policymodel.arriveStation != "":
        #地区码模式，需校验
            isDQM = True
            isSDS = False
        else:  
        #既不是三段模式，也不是地区码模式，需报错
            raise Exception("地址信息有误，请确认三段模式或地区码模式")
        if exMessage !="":
            raise Exception(exMessage)

        #单据唯一性
        remotedata = policy_model.remotedata.query.filter(policy_model.remotedata.appkey==postdata['appkey'], policy_model.remotedata.systemOrderId==postdata['sequencecode']).order_by(policy_model.remotedata.CreateDate.desc()).all()
        result = []
        dataresult = model_to_dict(remotedata)
        if postdata['action'] == "apply":
            if len(dataresult) > 0:   
                raise Exception("sequencecode已存在重复,请不要重复投递")
        # 地址校验
        if isDQM == True:
            startdistricts = districts_model.districts.query.filter(districts_model.districts.guid==postdata['startareacode'])
            result = []
            dataresult = model_to_dict(startdistricts)
            if len(dataresult)!=1:
                raise Exception("起运地地区码startareacode有误") 
            enddistricts = districts_model.districts.query.filter(districts_model.districts.guid==postdata['endareacode'])
            dataresult = model_to_dict(enddistricts)
            if len(dataresult)!=1:
                raise Exception("目的地地区码endareacode有误")

        if isSDS == True:
            startName = postdata['startprovince']
            startcity = postdata['startcity']
            startdistrict = postdata['startdistrict']
            if startcity!="":
                startName = startcity + "，" + startName
            if startdistrict != "":
                startName = startdistrict + "，" + startName
            endName = postdata['endprovince']
            endcity = postdata['endcity']
            if endcity != "":
                endName = endcity + "，" + endName
            isStartValid = False
            startData = districts_model.districts.query.filter(districts_model.districts.DisplayName == startName)
            dataresult = model_to_dict(startData)
            if len(dataresult) > 0:
                isStartValid = True
            else: 
            # 二级缩进
                startData = districts_model.districts.query.filter(districts_model.districts.DisplayName == startcity+'，'+startName)
                dataresult = model_to_dict(startData)
                if len(dataresult) > 0:
                    isStartValid = True
                else:
                    # 三级缩进
                        startData = districts_model.districts.query.filter(districts_model.districts.DisplayName == startName)
                        dataresult = model_to_dict(startData)
                        if len(dataresult) > 0:
                            isStartValid = True
                if isStartValid == False:
                    raise Exception("起运地省市区有误")
            isEndValid = False
            endData = districts_model.districts.query.filter(districts_model.districts.DisplayName == endName)
            dataresult = model_to_dict(endData)
            if len(dataresult) > 0:
                isEndValid = True
            else:
                # 二级缩进
                endData = districts_model.districts.query.filter(districts_model.districts.DisplayName == endcity+'，'+endName)
                dataresult = model_to_dict(endData)

                if len(dataresult) > 0:
                    isEndValid = True
                else:
                    #  三级缩进
                    endData = districts_model.districts.query.filter(districts_model.districts.DisplayName == endName)
                    dataresult = model_to_dict(endData)
                    if len(dataresult) > 0:
                        isEndValid = True
            if isEndValid == False:
                raise Exception("目的地省市区有误")
        # 产品信息校验
        productdata = GJXXPT_Product_model.GJXXPTProduct.query.filter(GJXXPT_Product_model.GJXXPTProduct.appkey == postdata['appkey'],GJXXPT_Product_model.GJXXPTProduct.InsuranceCoverageCode == postdata['productid'])
        # productdata = GJXXPT_Product_model.GJXXPTProduct.query.filter(GJXXPT_Product_model.GJXXPTProduct.appkey == '58054b759146320224dca9e58df873ad',GJXXPT_Product_model.GJXXPTProduct.InsuranceCoverageCode == 'LK046016')
        dataresult = model_to_dict(productdata)
        if len(dataresult) !=1:
            raise Exception("产品未配置!")
        product_deductible = dataresult[0]['deductible']
        product_insurancecoveragename = dataresult[0]['InsuranceCoverageName']
        product_transportcode = dataresult[0]['TransportModeCode']
        product_chargetypecode = dataresult[0]['ChargeTypeCode']
        product_cargotype = dataresult[0]['CargoTypeClassification1']
        if product_deductible == "按约定":
            policymodel.termContent = product_deductible
        if policymodel.mpObject == "按约定":
            policymodel.mpObject = product_insurancecoveragename
        if policymodel.trafficType == "按约定":
            policymodel.trafficType = product_transportcode
        if policymodel.mpRelation == "按约定":
            policymodel.mpRelation = product_chargetypecode
        if policymodel.cargoType == "按约定":
            policymodel.cargoType = product_cargotype
        if policymodel.termContent != product_deductible:
            raise Exception("免赔额与产品定义不符")
        if policymodel.mpObject != product_insurancecoveragename:
            raise Exception("险别名称与产品定义不符")
        if policymodel.trafficType != product_transportcode:
            raise Exception("运输方式编码与产品定义不符")
        if policymodel.mpRelation != product_chargetypecode:
            raise Exception("计费方式与产品定义不符")
        if policymodel.cargoType != product_cargotype:
            raise Exception("货物类型编码与产品定义不符")

        # 保费校验
        product_lowestpremium = dataresult[0]['MonetaryAmount']#最低保费
        product_rate = dataresult[0]['Rate'] #约定费率
        product_maxAmount = dataresult[0]['PolicyAmount'] #最高保额

        # 如果触发最低保费，则不校验费率，否则需校验

        if product_maxAmount !="":
            # 触发最高保额          
            m3 = decimal.Decimal(str(decimal.Decimal(policymodel.cargeValue))) - decimal.Decimal(str(decimal.Decimal(product_maxAmount)))
            if m3 > 0 :
                print(product_maxAmount)
                raise Exception("超过与保险公司约定的最高保额",product_maxAmount)
        if policymodel.claimLimit2 !="是" and policymodel.appkey == "84a54da7d11f45fecac39df7207bb216": #百世优货如果无需电子保单，则无最低保费
            product_lowestpremium = "0.01"
            # 如果触发最低保费，则不校验费率，否则需校验
        if decimal.Decimal(str(decimal.Decimal(policymodel.insuranceFee))) > decimal.Decimal(str(decimal.Decimal(product_lowestpremium))):
            # 保费=保额*费率
            _rate=decimal.Decimal(product_rate.split('%',1)[0])/100
            _premium = decimal.Decimal((decimal.Decimal(product_maxAmount) * _rate))
        if decimal.Decimal(str(decimal.Decimal(policymodel.insuranceFee))) !=_premium:
            raise Exception("保费计算有误")
        elif decimal.Decimal(str(decimal.Decimal(policymodel.insuranceFee))) < decimal.Decimal(str(decimal.Decimal(product_lowestpremium))):
            raise Exception("保费不能低于合同约定的最低保费")#触发最低保费
        # 获取子保单号
        if postdata['action'] == "apply":
            ZZ = postdata['solutionid']
            if postdata['action'] == "apply":
                sql = "select MAX(channelOrderId) from RemoteData WHERE policyNo='%s' AND appkey='%s'" %(ZZ, postdata['appkey'])
                dataresult = dal.SQLHelper.fetch_one(sql)
                if len(dataresult) == 0:
                    newPolicyNo = ZZ + "10000001"
                else:
                    if (dataresult[0][0] is None):
                        newPolicyNo = ZZ + "10000001"
                    else:
                        aaa = dataresult[0][0]
                        newPolicyNo = ZZ + str(int(str(aaa)[len(ZZ):8] + 1))
   
        
        policymodel.save()
        # 投递保险公司 或 龙琨编号
        # 反馈客户

        #写入日志
        log_file = 'E:/policyinterface2/logs/'+datetime.datetime.now().strftime("%Y-%m-%d")+'sendpolicy.log'
        log_format = '%(message)s'
        logging.basicConfig(filename=log_file, level=logging.WARNING, format=log_format)
        logger = logging.getLogger()
        logger.warning(str(postdata))

        result = {}
        result['responsecode'] = '1'
        result['responsemessage'] = '投保成功'
        result['applicationserial'] = '投保成功'
        resultReturn = json.dumps(result)
        return json.loads(resultReturn)

    except Exception as err:
        result = {}
        result['responsecode'] = '0'
        print(err)
        result['responsemessage'] = str(err)
        result['applicationserial'] = ''
        resultReturn = json.dumps(result)
        return json.loads(resultReturn)

# 投保接口 (聚盟)
@app.route('/jmpolicy', methods=['POST'])
def jmpolicy():
    cust_sequencecode = ''
    cust_appkey = ''
    from models import jm_ht_policy_model as policy_model
    from models import GJXXPT_Product_model
    from dals import dal
    from models import ValidInsured_model
    postdata = ""
    try:
        # 获取请求 
        postdata = json.loads(request.get_data(as_text=True))
        policymodel = policy_model.jm_ht_remotedata()
        
        newguid = str(uuid.uuid1())
        #写入日志
        log_file = open('logs/' + newguid +'_jmpolicy.log',mode='a')
        log_file.write('---------------------------收到客户报文 ' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '---------------------------\n')
        log_file.write(str(postdata) + '\n')
        log_file.close()
        # 保存客户运单
        policymodel.guid = newguid        
        # 头部信息
        policymodel.appkey = postdata['appkey']     
        cust_appkey = postdata['appkey']
        policymodel.bizContent = postdata['usercode']
        policymodel.channelOrderId = postdata['sequencecode']
        cust_sequencecode = postdata['sequencecode']
        policymodel.policyNo = postdata['solutionid']
        policymodel.claimLimit = postdata['productid']       

        action = postdata['action']  
        policymodel.Status = '等待投保'
        policymodel.CreateDate = datetime.datetime.now()
        # 投保主体
        policymodel.custCoName = postdata['applicantname']
        policymodel.custProperty = postdata['applicanttype']
        policymodel.custId = postdata['applicantidnumber']
        policymodel.insuredName = postdata['insuredname']
        policymodel.shipperProperty = postdata['insuredtype']        
        policymodel.shipperId = postdata['insuredidnumber']
        policymodel.shipperContact = postdata['spname']
        # 保险信息
        policymodel.cargeValue = postdata['policyamount']
        policymodel.policyRate = postdata['rate']
        policymodel.termContent = postdata['deductible']
        policymodel.insuranceFee = postdata['premium']
        policymodel.mpObject = postdata['insurancecoveragename']
        policymodel.mpRelation = postdata['chargetypecode']
        policymodel.departDateTime = postdata['insuredatetime']
        policymodel.shipId = postdata['originaldocumentnumber']
        policymodel.trafficType = postdata['transportmodecode']
        policymodel.licenseId = postdata['vehiclenumber']
        policymodel.departProvince = postdata['startprovince']
        policymodel.departCity = postdata['startcity']
        policymodel.departDistrict = postdata['startdistrict']
        policymodel.destinationProvice = postdata['endprovince']
        policymodel.destinationCity = postdata['endcity']
        policymodel.destinationDistrict = postdata['enddistrict']
        policymodel.departSpot = postdata['startaddress']
        policymodel.deliveryAddress = postdata['endaddress']
        policymodel.departStation = postdata['startareacode']
        policymodel.arriveStation = postdata['endareacode']
        policymodel.arriveProperty = postdata['transitaddress']
        policymodel.cargoName = postdata['descriptionofgoods']
        policymodel.cargoType = postdata['cargotype']
        policymodel.packageType = postdata['packagetype']
        policymodel.cargoCount = postdata['packagequantity']
        policymodel.cargoKind = postdata['packageunit']
        policymodel.cargoWeight = postdata['weight']
        policymodel.mpAmount = postdata['weightunit']
        policymodel.volume = postdata['volume']
        policymodel.mpRate = postdata['volumeunit']

        ValidInsured = ValidInsured_model.ValidInsured.query.filter(ValidInsured_model.ValidInsured.Appkey==policymodel.appkey,ValidInsured_model.ValidInsured.ValidInsuredName==policymodel.custCoName).all()
        ValidInsured = model_to_dict(ValidInsured)
        if len(ValidInsured)==0 :
            raise Exception('开票信息配置信息不存在，投保失败')
        #必填项校验
        exMessage = ''
        
        if policymodel.appkey == "":
            exMessage += "appkey不能为空;"
        if policymodel.bizContent == "":
            exMessage += "usercode不能为空;"
        # if policymodel.policyNo == "":
        #     exMessage += "solutionid不能为空;"
        if policymodel.channelOrderId == "":
            exMessage += "sequencecode不能为空;"
        if policymodel.claimLimit == "":
            exMessage += "productid不能为空;"
        # if action == "":
        #     exMessage += "action不能为空;"
        if policymodel.custCoName == "":
            exMessage += "applicantname不能为空;"                        
        if policymodel.custProperty == "":
            exMessage += "applicanttype不能为空;"
        if policymodel.custId == "":
            exMessage += "applicantidnumber不能为空;"
        if policymodel.insuredName == "":
            exMessage += "insuredName不能为空;"
        if policymodel.shipperProperty == "":
            exMessage += "insuredtype不能为空;"
        if policymodel.shipperId == "":
            exMessage += "insuredidnumber不能为空;"
        if policymodel.cargeValue == "":
            exMessage += "policyamount不能为空;"
        if policymodel.policyRate == "":
            exMessage += "rate不能为空;"
        if policymodel.termContent == "":
            exMessage += "deductible不能为空;"
        if policymodel.insuranceFee == "":
            exMessage += "premium不能为空;"
        if policymodel.mpObject == "":
            exMessage += "insurancecoveragename不能为空;"
        if policymodel.mpRelation == "":
            exMessage += "chargetypecode不能为空;"
        if policymodel.departDateTime == "":
            exMessage += "insuredatetime不能为空;"
        else:        
            if len(policymodel.departDateTime) != 14:
                raise Exception("错误：起运日期departDateTime格式有误，正确格式：20170526153733;")
            else:                   
                # 倒签单校验
                departDateTimes = policymodel.departDateTime
                policymodel.departDateTime = str(departDateTimes[0:4]) + "-" + str(departDateTimes[4:6]) + "-" + str(departDateTimes[6:8]) + " "+ str(departDateTimes[8:10]) + ":" + str(departDateTimes[10:12])+ ":" + str(departDateTimes[12:14])
                time = int(departDateTimes)+10000 # 加10000相当于一个小时
                now = int(datetime.datetime.now().strftime("%Y%m%d%H%M%S"))
                
                if time - now < 100: # 100相当于小于1分钟
                    exMessage += "当前不允许倒签单;"
                
        if policymodel.shipId == "" and policymodel.licenseId =="":
            exMessage += "运单号或者车牌号至少一个必填;"
        if policymodel.trafficType == "":
            exMessage += "transportmodecode不能为空;"
        # if policymodel.departSpot == "":
        #     exMessage += "startaddress不能为空;"
        # if policymodel.deliveryAddress == "":
        #     exMessage += "endaddress不能为空;"
        if policymodel.cargoName == "":
            exMessage += "descriptionofgoods不能为空;"
        if policymodel.cargoType == "":
            exMessage += "cargotype不能为空;"
        if policymodel.cargoCount == "":
            exMessage += "packagequantity不能为空;"
        if exMessage !="":
            raise Exception(exMessage)

        #单据唯一性
        remotedata = policy_model.jm_ht_remotedata.query.filter(policy_model.jm_ht_remotedata.appkey==postdata['appkey'], policy_model.jm_ht_remotedata.Status=='投保成功', policy_model.jm_ht_remotedata.channelOrderId==postdata['sequencecode']).order_by(policy_model.jm_ht_remotedata.CreateDate.desc()).all()
        print(remotedata)
        result = []
        dataresult = model_to_dict(remotedata)
        if postdata['action'] == "apply":
            if len(dataresult) > 0:   
                raise Exception("sequencecode已存在重复,请不要重复投递")


        # 产品信息校验
        productdata = GJXXPT_Product_model.GJXXPTProduct.query.filter(GJXXPT_Product_model.GJXXPTProduct.appkey == postdata['appkey'],
        GJXXPT_Product_model.GJXXPTProduct.InsuranceCoverageCode == postdata['productid'])
        dataresult = model_to_dict(productdata)
        if len(dataresult) !=1:
            raise Exception("产品未配置!")
        product_deductible = dataresult[0]['deductible']
        product_insurancecoveragename = dataresult[0]['InsuranceCoverageName']
        product_transportcode = dataresult[0]['TransportModeCode']
        product_chargetypecode = dataresult[0]['ChargeTypeCode']
        product_cargotype = dataresult[0]['CargoTypeClassification1']
        product_lowestpremium = dataresult[0]['MonetaryAmount']#最低保费
        product_rate = dataresult[0]['Rate'] #约定费率
        product_maxAmount = dataresult[0]['PolicyAmount'] #最高保额

        if policymodel.termContent == "按约定":
            policymodel.termContent = product_deductible
        if policymodel.mpObject == "按约定":
            policymodel.mpObject = product_insurancecoveragename
        if policymodel.trafficType == "按约定":
            policymodel.trafficType = product_transportcode
        if policymodel.mpRelation == "按约定":
            policymodel.mpRelation = product_chargetypecode
        if policymodel.policyRate == "按约定":
            policymodel.policyRate = product_rate
        if policymodel.cargoType == "按约定":
            policymodel.cargoType = product_cargotype

        if policymodel.termContent != product_deductible:
            raise Exception("免赔额与产品定义不符")
        if policymodel.mpObject != product_insurancecoveragename:
            raise Exception("险别名称与产品定义不符")
        if policymodel.trafficType != product_transportcode:
            raise Exception("运输方式编码与产品定义不符")
        if policymodel.mpRelation != product_chargetypecode:
            raise Exception("计费方式与产品定义不符")        
        if policymodel.cargoType != product_cargotype:
            raise Exception("货物类型编码与产品定义不符")

        # 保费校验 如果触发最低保费，则不校验费率，否则需校验

        if product_maxAmount != "" and float(product_maxAmount) != 0.00:
            # 触发最高保额          
            m3 = decimal.Decimal(str(decimal.Decimal(policymodel.cargeValue))) - decimal.Decimal(str(decimal.Decimal(product_maxAmount)))
            if m3 > 0 :
                raise Exception("超过与保险公司约定的最高保额",product_maxAmount)
            # 如果触发最低保费，则不校验费率，否则需校验
        if decimal.Decimal(str(decimal.Decimal(policymodel.insuranceFee))) >= decimal.Decimal(str(decimal.Decimal(product_lowestpremium))):
            # 保费=保额*费率
            _rate=decimal.Decimal(product_rate.split('%',1)[0])/100
            _premium = decimal.Decimal((decimal.Decimal(policymodel.cargeValue) * _rate))
            if decimal.Decimal(str(decimal.Decimal(policymodel.insuranceFee))) !=_premium:
                raise Exception("保费计算有误")
        else:
            raise Exception("保费不能低于合同约定的最低保费")#触发最低保费

        
        policymodel.save()

        # 投递保险公司 或 龙琨编号
        _Status, _InsurancePolicy, _PdfURL, _Msg, _Flag = postInsurer_HT(newguid)       
        if _Flag == "1":
            _Msg = "投保成功"
        result = {}
        result['responsecode'] = _Flag
        result['responsemessage'] = _Msg
        result['applicationserial'] = newguid
        result['appkey'] = policymodel.appkey
        result['sequencecode'] = policymodel.channelOrderId
        result['premium'] = policymodel.insuranceFee
        result['policyno'] = _InsurancePolicy
        result['downloadurl'] = _PdfURL
        resultReturn = json.dumps(result)
        return json.loads(resultReturn)
    
    except Exception as err:
        traceback.print_exc()
        result = {}
        result['responsecode'] = '0'
        result['responsemessage'] = str(err)
        result['applicationserial'] = ''
        result['appkey'] = cust_appkey
        result['sequencecode'] = cust_sequencecode
        result['premium'] = ''
        result['policyno'] = ''
        result['downloadurl'] = ''
        log_file = open('logs/' + newguid +'_jmpolicy.log',mode='a')
        log_file.write('---------------------------聚盟报错信息 ' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '---------------------------\n')
        log_file.write(str(err))
        log_file.close()
        sendAlertMail('manman.zhang@dragonins.com','聚盟—华泰投递出错',str(err)+'<br />' + str(postdata))
        resultReturn = json.dumps(result)
        return json.loads(resultReturn)

# 投保日志（聚盟）
@app.route('/jmpolicy/log', methods=['GET'])
def getlog_jm():
    # 需要知道2个参数, 第1个参数是本地目录的path, 第2个参数是文件名(带扩展名)
    directory = os.getcwd()  # 假设在当前目录
    print(directory)
    filename = 'uwsgi.log'
    return send_from_directory(os.path.join(directory, 'uwsgi'), filename, as_attachment=True)
    #return send_file('/root/wwwroot/policyinterface/uwsgi/uwsgi.log')

# 华泰出单接口(沙师弟)
def issueInterface(guid):
    from dals import dal
    from urllib import parse
    import requests
    FormData = ""
    try:
        sql = "select * from ssd_ht_remotedata left join validinsured on ssd_ht_remotedata.appkey = validinsured.Appkey where ssd_ht_remotedata.guid = '"+guid+"'"   
        row = dal.SQLHelper.fetch_one(sql,None)
        postdata={}
        channelObject = {}
        channelObject["bizCode"]= '101' # 交易类型
      
        channelObject["orderId"]= row[0]  # guid 
        channelObject["createTime"]= str(datetime.datetime.now())[0:19] # 当前时间

        insuranceObject = {}
        insuranceObject['plan'] = 'A' # 款别
        insuranceObject['srcCPlyNo'] = '' # 不必填
        insuranceObject['prmCur'] = '01' 
        insuranceObject['premium'] = row[21] # 保险费
        insuranceObject['amtCur'] = '01'
        insuranceObject['amount'] = '100000.0'
        productid = row[17] # 产品编号 
        if productid =="LK999999":
            # 测试环境
            channelObject["channelCode"]='100197' # 渠道编码
            key = "123456@HT" # 线下提供的密钥
            channelObject["channelName"]='上海励琨互联网科技有限公司' # 渠道名称
            insuranceObject["insuranceName"] = '承运人公路货运责任保险条款 ' # 产品名称
            url="http://219.141.242.74:9039/service_platform/InsureInterface"
        else:
            # 正式环境
            channelObject["channelCode"]='100197' # 渠道编码
            key = "shlkssd2020@HT" # 线下提供的密钥
            channelObject["channelName"]='上海励琨-沙师弟' # 渠道名称
            insuranceObject["insuranceName"] = '上海励琨-沙师弟公路承运险 ' # 产品名称
            url="http://219.141.242.74:9004/service_platform/InsureInterface"

        # insuranceObject['rate'] = str(decimal.Decimal(row[68][:-1]) * 10) # policyRate 去除百分号后乘以10 [:-1] 截取从头开始到倒数第一个字符之前
        if productid == "LK801001" or productid =="LK999999":    
            now = datetime.datetime.now()
            insuranceObject['effectiveTime'] = (now- datetime.timedelta(hours=now.hour, minutes=now.minute, seconds=now.second,microseconds=now.microsecond)+datetime.timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S") # 次日凌晨    
            insuranceObject['terminalTime'] = str(datetime.datetime.strptime(insuranceObject['effectiveTime'],'%Y-%m-%d %H:%M:%S')+ datetime.timedelta(days = 30)) # 保期30天
            insuranceObject["insuranceCode"] = '362104' # 险种代码

        if productid == "LK801002":
            insuranceObject['effectiveTime'] = row[36]# 保险起期 departDateTime
            insuranceObject['terminalTime'] = str(datetime.datetime.strptime(insuranceObject['effectiveTime'],'%Y-%m-%d %H:%M:%S')+ datetime.timedelta(days = 2)) # 保险至期+2天
            insuranceObject["insuranceCode"] = '362208' 
    
            
        insuranceObject['copy'] = '1' # 份数 
        insuranceObject['docType'] = '' # 不必填
        insuranceObject['docSN'] = '' # 不必填

        # 需要提供开票的六项信息
        appntObject = {}
        # appntObject["appName"] = row[3] # 投保人姓名 custCoName
        appntObject["appName"] = '沙师弟（重庆）网络科技有限公司及其子公司、分公司' # 投保人名称
        appntObject["appType"] = '2' 
        appntObject["appEmail"] = '' # 不必填
        appntObject["appGender"] = '' # 不必填
        appntObject["appIDType"] = '97' 

        appntObject["appNumber"] = row[79] # 投保人证件号(取validinsured表的CertfCde字段)

        appntObject["appTelNumber"] = row[82] # 投保人电话号(取validinsured表的tel字段)
        appntObject["appAddr"] = row[86]# 地址信息(取validinsured表的DetailAddress字段)
        appntObject["appContact"] = row[9] # 司机名称 insuredname
        appntObject["isTaxInvoice"] = '1' 
        appntObject["taxCertifi"] = row[79] # 税务登记证号(取validinsured表的CertfCde字段)
        appntObject["depositBank"] = row[78] # 开户银行
        appntObject["bankAccount"] = row[77] # 银行账户

        insuredObject = {}
        insuredObject["insuredName"] = '沙师弟（重庆）网络科技有限公司及其子公司、分公司' # 被保险人名称
        insuredObject["insuredType"] = '2' # 
        insuredObject["insuredEmail"] = '' # 不必填
        insuredObject["InsuredGender"] = '' # 不必填
        insuredObject["insuredIDType"] = '06' # 被保人证件类型


        insuredObject["insuredNumber"] = appntObject["appNumber"]# 被保人证件号
        insuredObject["insuredTelNumber"] = appntObject["appTelNumber"] # 被保险人电话
        insuredObject["insuredAddress"] = appntObject["appAddr"] # 不必填
        insuredObject["relationship"] = '' # 不必填
        insuredObject["isLegal"] = '1' # 
        definedSafeObj = {}
        definedSafeObj["isDefinedSafe"] = '0' # 默认0，此节点不用传

        payObject = {} # 支付信息
        payObject["isSinglePay"] = '0' # 是否逐单支付
            
        agreementObject = {}
        agreementObject["policyDeductible"] = '1）针对一般事故：每一运输工具每次事故人民币5000元或损失金额的10%，以高者为准；2）针对火灾、爆炸及运输工具倾覆或追尾他车：每一运输工具每次事故人民币10000元或损失金额的20%，以高者为准。' # 免赔额/率
        agreementObject["policySpec"] =  "1）被保险人在运输过程中，由于盗窃造成货物的损失，依法应由被保险人承担赔偿责任的，保险人按本保险合同约定负责赔偿。2）对于裸装货物、二手货（旧货）、退货及返修货物，本保险仅承保基本险的风险；3）本保险不负责任何形式的仓储期间的损失，但运输过程中的临时仓储除外；4）承运车辆须具备合格驾驶证、行驶证及营运许可证，否则，保险人不负赔偿责任； 5） 保险人放弃对以下车辆的代位追偿：赣CB6506，赣CB2613，皖CA4152，皖AD3005，赣E66415，赣E43782，赣E66043，粤ACN608，粤ACY261，粤AAK050；但不放弃对任何其它第三方责任人追偿的权利；6）本保单扩展承保目的地为乌鲁木齐的货物。7）未尽事宜以协议（MC9033622202004001）为准" # 特别约定

        productDiffObject = {}
        productDiffObject["reMark"] = '' # 默认为空
        productDiffObject["vehicleNum"] = row[15] # 车牌号 licenseid
        productDiffObject["vehicleModel"] = '*'
        productDiffObject["vehicleLen"] = '*'
        productDiffObject["vehicleFrameNum"] = '*' 
        productDiffObject["goodsName"] = str(row[27])+',运单号：'+ str(row[14]) # 货物名称 cargoName

        productDiffObject["goodsQuantity"] = row[37] # 货物数量 cargoCount
        productDiffObject["goodsPack"] = '08' # 包装方式
        productDiffObject["goodsValue"] = row[18] # 货物价值 cargeValue

        productDiffObject["goodsType"] = "SX001423" # 货物大类
        productDiffObject["goodsTypeSec"] = "SX00140105" # 二级货物明细
        
        productDiffObject["transFrom"] = row[28]+row[29]+row[30] #  省、市、区（departProvince + departCity + departDistrict）
        productDiffObject["transDepot"] = row[46] # 中转地
        # 校验中转地
        if len(productDiffObject["transDepot"])>100:
            productDiffObject["transDepot"] = "见运单"

        productDiffObject["transTo"] = row[42] # 目的地 deliveryAddress
        productDiffObject["transDate"] = row[36] # 起运日期
        if productid == "LK801001" or productid =="LK999999": # 客户投保30天的期单
            productDiffObject["transportCost"] = ""

        if productid == "LK801002": # 次单
            productDiffObject["transportCost"] = row[18] # 运费

        productDiffObject["insuredContact"] = row[9] # 被保人联系人
        productDiffObject["insuredTel"] = row[7] # 被保人联系方式

        postdata["channelObject"] = channelObject
        postdata["insuranceObject"] = insuranceObject
        postdata["appntObject"] = appntObject
        postdata["insuredObject"] = [insuredObject]
        postdata["definedSafeObj"] = definedSafeObj
        postdata['payObject'] = payObject
        postdata["agreementObject"] = agreementObject
        postdata["productDiffObject"] = productDiffObject
        Json = json.dumps(postdata, ensure_ascii=False)
        Json2 = Json.replace("%", "%25").replace("&", "%26").replace("\\+", "%2B")
        # print(Json)          
        # key = "123456@HT" # 线下提供的密钥
        m = hashlib.md5()
        b = (str(Json2) + key).encode(encoding='utf-8')
        m.update(b)
        signmd5 = m.hexdigest()
        # print(signmd5)

        #写入日志
    
        log_file = open('logs/' + guid +'_ssdpolicy.log',mode='a', encoding='utf-8')
        log_file.write('---------------------------发给华泰报文 ' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '---------------------------\n')
        log_file.write(str(Json)+'\n')
        #log_file.write(signmd5)
            
        
        # 通过字典方式定义请求body
        FormData = {"json": str(Json), "channelCode": str(channelObject["channelCode"]), "signature": str(signmd5)}
        data = parse.urlencode(FormData)
        headers = {
            'Content-Type': "application/x-www-form-urlencoded",
        }
        # 请求方式
        content = requests.post(url=url, headers=headers, data=data).text
        # print(content)
        content = json.loads(content)
        log_file.write('---------------------------对接华泰结果 ' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '---------------------------\n')
        log_file.write(str(content))
        log_file.close()
        
        _bizCode = "" # 业务类型
        # _responseCode = "" # 错误代码
        _Flag = "" # 错误代码
        # _responseInfo = "" # 错误信息
        _Msg = "" # 错误信息
        _channelCode = "" # 渠道编码
        _orderId = "" # 订单号
        _totalPremium = "" # 总保费
        # _policyNO = "" # 保单号
        _InsurancePolicy = "" # 保单号
        # _policyURL = "" # 电子保单地址
        _PdfURL = "" # 电子保单地址
        _Flag = content['responseCode'] # 接收返回的参数
        guiderr = 'guid：'+guid
        error = '报错信息：' + str(content)
        if _Flag == "2": # 人工核保
            _bizCode = content['bizCode'] 
            _Msg = content['responseInfo'] 
            _Status = "人工核保" 
            sendAlertMail('manman.zhang@dragonins.com','沙师弟-对接华泰',str(guiderr) + '<br />' + str(error)) 
        elif _Flag == "1": # 核保通过
            _bizCode = content['bizCode'] 
            _Msg = content['responseInfo'] 
            _channelCode = content['channelCode'] 
            _orderId = content['orderId'] 
            _totalPremium = content['totalPremium']
            _InsurancePolicy = content['policyNO'] 
            _PdfURL = content['policyURL'] 
            _Status = "投保成功" 
        else: # 投保失败
            _bizCode = content['bizCode'] 
            _Msg = content['responseInfo'] 
            _Status = "投保失败" 
            sendAlertMail('manman.zhang@dragonins.com','沙师弟-对接华泰',str(guiderr) + '<br />' + str(error)) 
        # 回写投保表
        sql = "UPDATE ssd_ht_remotedata SET Status='%s', errLog='%s', bizContent='%s', custId='%s',relationType='%s' WHERE guid='%s'" %(_Status, _Msg, _InsurancePolicy, _orderId, _PdfURL, guid)
        dal.SQLHelper.update(sql,None)
        return _Status, _InsurancePolicy, _PdfURL, _Msg, _Flag
    except Exception as err:
        traceback.print_exc()
        print("请求失败",err)
        sendAlertMail('manman.zhang@dragonins.com','华泰投递出错',str(err)+'<br />' + str(FormData))

# 投保接口 (沙师弟)
@app.route('/ssdpolicy', methods=['POST'])
def ssdpolicy():
    cust_sequencecode = ''
    cust_appkey = ''
    from models import ssd_ht_policy_model as policy_model
    from models import GJXXPT_Product_model
    from dals import dal
    from models import ValidInsured_model
    import time
    try:
        # 获取请求 
        postdata = json.loads(request.get_data(as_text=True))
        policymodel = policy_model.ssd_ht_remotedata()
        newguid = str(uuid.uuid1())
        #写入日志
        log_file = open('logs/' + newguid +'_ssdpolicy.log',mode='a', encoding='utf-8')
        log_file.write('---------------------------收到客户报文 ' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '---------------------------\n')
        log_file.write(str(postdata) + '\n')
        log_file.close()
        # 保存客户运单
        policymodel.guid = newguid        
        # 头部信息
        policymodel.appkey = postdata['appkey']     
        cust_appkey = postdata['appkey']
        policymodel.bizContent = postdata['usercode']
        policymodel.channelOrderId = postdata['sequencecode']
        cust_sequencecode = postdata['sequencecode']
        policymodel.policyNo = postdata['solutionid']
        policymodel.claimLimit = postdata['productid']
        action = postdata['action']  
        policymodel.Status = '等待投保'
        policymodel.CreateDate = datetime.datetime.now()
        # 投保主体
        policymodel.custCoName = postdata['applicantname']
        policymodel.custProperty = postdata['applicanttype']
        policymodel.custId = postdata['applicantidnumber']
        policymodel.insuredName = postdata['insuredname']
        policymodel.shipperProperty = postdata['insuredtype']        
        policymodel.shipperId = postdata['insuredidnumber']
        policymodel.shipperContact = postdata['spname']
        # 保险信息
        policymodel.cargeValue = postdata['policyamount']
        policymodel.policyRate = postdata['rate']
        policymodel.termContent = postdata['deductible']
        policymodel.insuranceFee = postdata['premium']
        policymodel.mpObject = postdata['insurancecoveragename']
        policymodel.mpRelation = postdata['chargetypecode']
        policymodel.departDateTime = postdata['insuredatetime']
        policymodel.shipId = postdata['originaldocumentnumber']
        policymodel.trafficType = postdata['transportmodecode']
        policymodel.licenseId = postdata['vehiclenumber']
        policymodel.departProvince = postdata['startprovince']
        policymodel.departCity = postdata['startcity']
        policymodel.departDistrict = postdata['startdistrict']
        policymodel.destinationProvice = postdata['endprovince']
        policymodel.destinationCity = postdata['endcity']
        policymodel.destinationDistrict = postdata['enddistrict']
        policymodel.departSpot = postdata['startaddress']
        policymodel.deliveryAddress = postdata['endaddress']
        policymodel.departStation = postdata['startareacode']
        policymodel.arriveStation = postdata['endareacode']
        policymodel.transitSpot = postdata['transitaddress']
        policymodel.cargoName = postdata['descriptionofgoods']
        policymodel.cargoType = postdata['cargotype']
        policymodel.packageType = postdata['packagetype']
        policymodel.cargoCount = postdata['packagequantity']
        policymodel.cargoKind = postdata['packageunit']
        policymodel.cargoWeight = postdata['weight']
        policymodel.mpAmount = postdata['weightunit']
        policymodel.volume = postdata['volume']
        policymodel.mpRate = postdata['volumeunit']

        #必填项校验
        exMessage = ''
        
        if policymodel.appkey == "":
            exMessage += "appkey不能为空;"
        if policymodel.bizContent == "":
            exMessage += "usercode不能为空;"
        # if policymodel.policyNo == "":
        #     exMessage += "solutionid不能为空;"
        if policymodel.channelOrderId == "":
            exMessage += "sequencecode不能为空;"
        if policymodel.claimLimit == "":
            exMessage += "productid不能为空;"
        if action == "":
            exMessage += "action不能为空;"
        if policymodel.custCoName == "":
            exMessage += "applicantname不能为空;"                        
        if policymodel.custProperty == "":
            exMessage += "applicanttype不能为空;"
        if policymodel.custId == "":
            exMessage += "applicantidnumber不能为空;"
        if policymodel.insuredName == "":
            exMessage += "insuredName不能为空;"
        if policymodel.shipperProperty == "":
            exMessage += "insuredtype不能为空;"
        if policymodel.shipperId == "":
            exMessage += "insuredidnumber不能为空;"

        if policymodel.claimLimit != "LK801001" and policymodel.claimLimit != "LK999999": # 产品编号为LK801001时不校验

            if policymodel.shipId == "":
                exMessage += "originaldocumentnumber不能为空;"            
            if policymodel.cargeValue == "":
                exMessage += "policyamount不能为空;"
            # if policymodel.policyRate == "":
            #     exMessage += "rate不能为空;"
            if policymodel.departSpot == "":
                exMessage += "startaddress不能为空;"
            if policymodel.deliveryAddress == "":
                exMessage += "endaddress不能为空;"
            if policymodel.cargoName == "":
                exMessage += "descriptionofgoods不能为空;"                
            if policymodel.cargoType == "":
                exMessage += "cargotype不能为空;"
            if policymodel.cargoCount == "":
                exMessage += "packagequantity不能为空;" 
            if policymodel.departDateTime == "":
                exMessage += "insuredatetime不能为空;"
            else:        
                if len(policymodel.departDateTime) != 14:
                    raise Exception("错误：起运日期departDateTime格式有误，正确格式：20170526153733;")
                else:                   
                    # 倒签单校验
                    departDateTimes = policymodel.departDateTime
                    try:
                        time.strptime(departDateTimes, "%Y%m%d%H%M%S") # 校验日期是否合法
                        policymodel.departDateTime = str(departDateTimes[0:4]) + "-" + str(departDateTimes[4:6]) + "-" + str(departDateTimes[6:8]) + " "+ str(departDateTimes[8:10]) + ":" + str(departDateTimes[10:12])+ ":" + str(departDateTimes[12:14])
                        time = int(departDateTimes)+10000 # 加10000相当于一个小时
                        now = int(datetime.datetime.now().strftime("%Y%m%d%H%M%S"))

                        if time - now < 100: # 100相当于小于1分钟
                            exMessage += "当前不允许倒签单;" 
                    except:
                        exMessage += "起运日期不合法;"                                 

        if policymodel.termContent == "":
            exMessage += "deductible不能为空;"
        if policymodel.insuranceFee == "":
            exMessage += "premium不能为空;"
        if policymodel.mpObject == "":
            exMessage += "insurancecoveragename不能为空;"
        if policymodel.mpRelation == "":
            exMessage += "chargetypecode不能为空;"
   
                
        if policymodel.licenseId =="" or policymodel.licenseId == None:
            exMessage += "车牌号不能为空;"
        if policymodel.trafficType == "":
            exMessage += "transportmodecode不能为空;"  

        if exMessage !="":
            raise Exception(exMessage)

        #单据唯一性
        remotedata = policy_model.ssd_ht_remotedata.query.filter(policy_model.ssd_ht_remotedata.appkey==postdata['appkey'],policy_model.ssd_ht_remotedata.Status == '投保成功', policy_model.ssd_ht_remotedata.channelOrderId==postdata['sequencecode']).order_by(policy_model.ssd_ht_remotedata.CreateDate.desc()).all()
        result = []
        dataresult = model_to_dict(remotedata)
        if postdata['action'] == "apply":
            if len(dataresult) > 0:   
                raise Exception("sequencecode已存在重复,请不要重复投递")


        # 产品信息校验
        productdata = GJXXPT_Product_model.GJXXPTProduct.query.filter(GJXXPT_Product_model.GJXXPTProduct.appkey == postdata['appkey'],
        GJXXPT_Product_model.GJXXPTProduct.InsuranceCoverageCode == postdata['productid'])
        dataresult = model_to_dict(productdata)
        if len(dataresult) !=1:
            raise Exception("产品未配置!")
        product_deductible = dataresult[0]['deductible']
        product_insurancecoveragename = dataresult[0]['InsuranceCoverageName']
        product_transportcode = dataresult[0]['TransportModeCode']
        product_chargetypecode = dataresult[0]['ChargeTypeCode']
        product_cargotype = dataresult[0]['CargoTypeClassification1']

        product_rate = dataresult[0]['Rate'] #约定费率
        # product_maxAmount = dataresult[0]['PolicyAmount'] #最高保额

        if policymodel.termContent == "按约定":
            policymodel.termContent = product_deductible
        if policymodel.mpObject == "按约定":
            policymodel.mpObject = product_insurancecoveragename
        if policymodel.trafficType == "按约定":
            policymodel.trafficType = product_transportcode
        if policymodel.mpRelation == "按约定":
            policymodel.mpRelation = product_chargetypecode
        if policymodel.policyRate == "按约定":
            policymodel.policyRate = product_rate
        if policymodel.cargoType == "按约定":
            policymodel.cargoType = product_cargotype

        if policymodel.termContent != product_deductible:
            raise Exception("免赔额与产品定义不符")
        if policymodel.mpObject != product_insurancecoveragename:
            raise Exception("险别名称与产品定义不符")
        if policymodel.trafficType != product_transportcode:
            raise Exception("运输方式编码与产品定义不符")
        if policymodel.mpRelation != product_chargetypecode:
            raise Exception("计费方式与产品定义不符")        
        # if policymodel.cargoType != product_cargotype:
        #     raise Exception("货物类型编码与产品定义不符")

        # if product_maxAmount != "" and float(product_maxAmount) != 0.00:
        #     # 触发最高保额          
        #     m3 = decimal.Decimal(str(decimal.Decimal(policymodel.cargeValue))) - decimal.Decimal(str(decimal.Decimal(product_maxAmount)))
        #     if m3 > 0 :
        #         raise Exception("超过与保险公司约定的最高保额",product_maxAmount)

        # 产品编号校验
        # if policymodel.claimLimit == "LK801001":
        #     if float(policymodel.insuranceFee) != 25.00:
        #         raise Exception("30天期单的保费固定为25.00元")                  
        # elif policymodel.claimLimit == "LK801002":
        #     if float(policymodel.insuranceFee) != 1.00:
        #         raise Exception("次单的保费固定为1.00元")
        # else:
        #     raise Exception("测试环境暂时关闭")

        if policymodel.claimLimit == "LK801001" or policymodel.claimLimit == "LK999999" :
            if float(policymodel.insuranceFee) != 25.00:
                raise Exception("30天期单的保费固定为25.00元")                  
        if policymodel.claimLimit == "LK801002":
            if float(policymodel.insuranceFee) != 1.00:
                raise Exception("次单的保费固定为1.00元")

        
        policymodel.save()
        # 投递保险公司 或 龙琨编号

        _Status, _InsurancePolicy, _PdfURL, _Msg, _Flag = issueInterface(newguid)     

        if _Flag == "1":
            _Msg = "投保成功"
        result = {}
        result['responsecode'] = _Flag
        result['responsemessage'] = _Msg
        result['applicationserial'] = newguid
        result['appkey'] = policymodel.appkey
        result['sequencecode'] = policymodel.channelOrderId
        result['premium'] = policymodel.insuranceFee
        result['policyno'] = _InsurancePolicy
        result['downloadurl'] = _PdfURL
        resultReturn = json.dumps(result)
        return json.loads(resultReturn)

    except Exception as err:
        traceback.print_exc()
        result = {}
        result['responsecode'] = '0'
        result['responsemessage'] = str(err)
        result['applicationserial'] = ''
        result['appkey'] = cust_appkey
        result['sequencecode'] = cust_sequencecode
        result['premium'] = ''
        result['policyno'] = ''
        result['downloadurl'] = ''

        log_file = open('logs/' + newguid +'_ssdpolicy.log',mode='a',encoding='utf-8')
        log_file.write('---------------------------沙师弟报错信息 ' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '---------------------------\n')
        log_file.write(str(err))
        log_file.close()
        sendAlertMail('manman.zhang@dragonins.com','沙师弟—华泰投递出错',str(err)+'<br />' + str(postdata))
        resultReturn = json.dumps(result)
        return json.loads(resultReturn)

# 德坤接口
@app.route('/dekun', methods = ['POST'])
def dekun(): 
    from dals import dal
    from models import policy_model
    from models import GJXXPT_Product_model
    from models import districts_model
    postdata=""
    try:
        remoteuser = request.values['remoteuser'] # 接收传参
        # print(remoteuser)
        datatext = request.data.decode('utf-8') # 接收文本并解决中文乱码
        postdata = json.loads(datatext[10:]) # 截取文本并转json
        policymodel = policy_model.remotedata()
        # print("json数据",postdata)
        policymodel = policy_model.remotedata()
        policymodel.guid = str(uuid.uuid1())
        policymodel.channelOrderId = postdata['SequenceCode']
        policymodel.Status = postdata['status']
        policymodel.CreateDate = datetime.datetime.now()
        policymodel.policyNo = postdata['InsuranceBillCode'] # 大保单号
        policymodel.shipId = postdata['shipId']

        policymodel.appkey = remoteuser # 大保单号
        policymodel.custId = '1' # 投保人的Id
        PartyInformation = postdata['PartyInformation'] # 获取数组
        policymodel.custCoName = PartyInformation[0]['PartyName'] # 投保人名称
        policymodel.custUserName = PartyInformation[0]['UnitName'] # 投保人姓名
        policymodel.departSpot = PartyInformation[0]['UnitAddress']
        policymodel.custPhone = PartyInformation[0]['UnitPhone'] # 投保人电话
        policymodel.custEmail = PartyInformation[0]['UnitEmail'] # 投保人邮箱地址
        policymodel.insuredName = PartyInformation[1]['PartyName'] # 被保人姓名
        policymodel.deliveryAddress = PartyInformation[1]['UnitAddress']
        ChargeInformation = postdata['ChargeInformation']
        policymodel.departDateTime = ChargeInformation[0]['InsureDateTime'] # 预计起运时间 20170526153733
        policymodel.cargeValue = ChargeInformation[0]['SumInsuredMonetaryAmount'] # 货物价值
        policymodel.policyRate = ChargeInformation[0]['Rate'] # 费率
        policymodel.insuranceFee = ChargeInformation[0]['MonetaryAmount'] #保费
        TransportInformation = postdata['TransportInformation']
        policymodel.systemOrderId = TransportInformation[0]['OriginalDocumentNumber']
        policymodel.shippingType = TransportInformation[0]['ShippingType'] # 装载方式
        policymodel.lotType = TransportInformation[0]['LotType'] # 整车/零担
        PlaceOrLocationInformation = TransportInformation[0]['PlaceOrLocationInformation']
        policymodel.transitSpot = PlaceOrLocationInformation[0]['PlaceOrLocation'] # 中转地
        policymodel.departProvince = PlaceOrLocationInformation[0]['PlaceProvince'] # 出发地-省
        policymodel.destinationCity = PlaceOrLocationInformation[0]['PlaceCity'] # 到达地-市?
        GoodsInformation = TransportInformation[0]['GoodsInformation']
        policymodel.cargoName = GoodsInformation[0]['DescriptionOfGoods'] # 货物名称
        policymodel.cargoType = GoodsInformation[0]['CargoTypeClassification1'] # 货物大类
        policymodel.packageType = GoodsInformation[0]['PackageType'] # 包装类型
        policymodel.cargoCount = GoodsInformation[0]['PackageQuantity'] # 货物数量
        policymodel.trafficType = TransportInformation[0]['TransportModeCode'] # 运输方式
        policymodel.licenseId = TransportInformation[0]['VehicleNumber'] # 车辆编号
        policymodel.stealFlag = 'Y' # 附加盗窃标志
        policymodel.departCity = postdata['departCity']
        policymodel.destinationDistrict = postdata['destinationDistrict']
        policymodel.cargoWeight = postdata['cargoWeight']

        dragoninsProductCode =  ChargeInformation[0]['InsuranceCoverageCode'] #龙琨产品编号

        # remoteuser 判断
        if remoteuser !="8CB22A57-50E6-4B4C-9F65-BA45B5D56F9D":
            raise Exception("接口参数被拒绝")
        # 必填项校验
        

        if dragoninsProductCode =="":
            raise Exception("产品编码InsuranceCoverageCode必须传递")
        if PartyInformation[0]['PartyFunctionCode'] == "BM":
            if policymodel.custCoName == "":
                raise Exception("投保人的PartyName必须传递")
            if PartyInformation[0]['LogisticsExchangeCode'] == "":
                raise Exception("投保人的LogisticsExchangeCode必须传递")
            if PartyInformation[0]['CodeListQualifier'] == "":
                raise Exception("投保人的CodeListQualifier必须传递")                
        if PartyInformation[1]['PartyFunctionCode'] == "BN":
            if policymodel.insuredName == "":
                raise Exception("被保险人的PartyName必须传递")
            if PartyInformation[1]['CodeListQualifier'] == "":
                raise Exception("投保人的CodeListQualifier必须传递")

        # channelOrderId重复性校验
        existData = policy_model.remotedata.query.filter(policy_model.remotedata.appkey==remoteuser, policy_model.remotedata.channelOrderId==postdata['SequenceCode'])
        dataresult = model_to_dict(existData)
        if len(dataresult) > 0 :
            raise Exception("单据编号SequenceCode重复，不能重复投递！")
        if policymodel.cargoType == "" or policymodel.cargoType.lower() == "null":
            raise Exception("错误：CargoTypeClassification1必须传递且不能为空;")
        if policymodel.cargoName == "" or policymodel.cargoName.lower() == "null":
            raise Exception("错误：DescriptionOfGoods必须传递且不能为空;")
        if policymodel.packageType == "" or policymodel.packageType.lower() == "null":
            raise Exception("错误：PackageType必须传递且不能为空;")
        if policymodel.departDateTime == "":
            raise Exception("InsureDateTime必须传递")
        if policymodel.cargeValue == "":
            raise Exception("SumInsuredMonetaryAmount必须传递")
        if policymodel.insuranceFee == "":
            raise Exception("MonetaryAmount必须传递")
        # 根据平台账号和产品编号，获取产品信息，用于按协议方式取值，及产品信息核对
 

        sql = "select * from gjxxpt_product WHERE appkey='%s' AND InsuranceCoverageCode='%s'" %(remoteuser, dragoninsProductCode)
        dataresult = dal.SQLHelper.fetch_one(sql)
        # productData = GJXXPT_Product_model.GJXXPTProduct.query.filter(GJXXPT_Product_model.GJXXPTProduct.appkey == remoteuser ,GJXXPT_Product_model.GJXXPTProduct.InsuranceCoverageCode == dragoninsProductCode)
        # print(productData)
        # dataresult = model_to_dict(dataresult)
        # 与龙琨产品校对
        if len(dataresult) == 0:
            raise Exception("指定的InsuranceCoverageCode（险别代码）: " , dragoninsProductCode,"不存在，请联系龙琨系统管理员")

        _InsuranceCode = dataresult[0][4]
        _InsuranceCoverageName = dataresult[0][5]
      

        _ChargeTypeCode = dataresult[0][6]
        _deductible = dataresult[0][8]
        _MonetaryAmount = dataresult[0][9]
        _CargoTypeClassification1 = dataresult[0][10]
        
        _TransportModeCode = dataresult[0][12]
        if ChargeInformation[0]['InsuranceCode'] != _InsuranceCode:
            raise Exception("险种代码(InsuranceCode)与龙琨产品定义不一致")
        if ChargeInformation[0]['InsuranceCoverageName'] != _InsuranceCoverageName:
            raise Exception("险别名称(InsuranceCoverageName)与龙琨产品定义不一致")
        if ChargeInformation[0]['ChargeTypeCode'] != _ChargeTypeCode:
            raise Exception("计费方式代码(ChargeTypeCode)与龙琨产品定义不一致")
        # 指定字符串或不在指定范围内时，返回-1
        if policymodel.policyRate.find('%') < 0:
            raise Exception("费率(Rate)格式有误，应该带百分号，如：0.02%")
        Rate = policymodel.policyRate # 获取费率
        ReturnIndex = Rate.find('%')  # 返回索引
        custrate = decimal.Decimal(Rate[0:ReturnIndex])/100
        custpremium = decimal.Decimal(policymodel.insuranceFee)
        custamount = decimal.Decimal(policymodel.cargeValue)

        if ChargeInformation[0]['deductible'] != _deductible:
            raise Exception("免赔额(deductible)与龙琨产品定义不一致")
        # 客户保额*费率=客户保费   客户保费>=龙琨最低保费（倒算规则要给客户）
        if (decimal.Decimal(custpremium)) != 0.01:
            if decimal.Decimal(round(custamount * custrate,2)) != custpremium: # round(a, 2)四舍五入保留两位小数
                raise Exception("保额乘以费率不等于保费")
            if custpremium < decimal.Decimal(_MonetaryAmount):
                raise Exception("保费(MonetaryAmount)不能低于最低保费")
            if policymodel.cargoType != _CargoTypeClassification1:
                raise Exception("货物类型大类(CargoTypeClassification1)与龙琨产品定义不一致")
            if policymodel.trafficType != _TransportModeCode:
                raise Exception("运输方式编码(TransportModeCode)与龙琨产品定义不一致")

        if PlaceOrLocationInformation[0]['PlaceLocationQualifier'] == "5":
            detailDeparture = policymodel.transitSpot # 出发地详细地址
            departProvince = policymodel.departProvince # 出发地-省
            departCity = policymodel.destinationCity # 出发地-市
            departDistrict = PlaceOrLocationInformation[0]['PlaceDistrict'] # 出发地-县
            _departProvince = departProvince
        if PlaceOrLocationInformation[1]['PlaceLocationQualifier'] == "8":
            detailDestination = PlaceOrLocationInformation[1]['PlaceOrLocation'] # 到达地详细地址
            destinationProvice = PlaceOrLocationInformation[1]['PlaceProvince'] # 到达地-省
            destinationCity = PlaceOrLocationInformation[1]['PlaceCity'] # 到达地-市?
            destinationDistrict = PlaceOrLocationInformation[1]['PlaceDistrict'] # 到达地-县?
            _destinationProvice = destinationProvice

        consigneeName = "" # 收货人名称
        consigneePhone = "" # 收货人电话    
        cargoCount = policymodel.cargoCount # 货物数量
        cargoWeight = cargoCount + GoodsInformation[0]['PackageUnit']
        # decimal.Decimal lowestInsuranceFee = 0
        if policymodel.policyNo == "" or policymodel.policyNo.lower() == "null":
            raise Exception("错误：InsuranceBillCode必须传递且不能为空; ")
        if policymodel.licenseId == "":
            if policymodel.systemOrderId == "" or policymodel.systemOrderId.lower() == "null":
                raise Exception("错误：OriginalDocumentNumber必须传递且不能为空; ")
        else:
            if policymodel.systemOrderId == "" or policymodel.systemOrderId.lower() == "null":
                policymodel.systemOrderId = TransportInformation[0]['OriginalDocumentNumber']
        if policymodel.custCoName == "" or policymodel.custCoName.lower() == "null":
            raise Exception("错误：投保人的PartyName必须传递且不能为空; ")
        if policymodel.insuredName == "" or policymodel.insuredName.lower() == "null":
            raise Exception("错误：被保险人的PartyName必须传递且不能为空; ")
        if policymodel.cargeValue == "" or policymodel.cargeValue.lower() == "null":
            raise Exception("错误：SumInsuredMonetaryAmount必须传递且不能为空;")
        if policymodel.insuranceFee == "" or policymodel.insuranceFee.lower() == "null":
            raise Exception("错误：MonetaryAmount必须传递且不能为空; ")
        else:
            # 最低保费控制
            if decimal.Decimal(policymodel.insuranceFee) < 0:
                raise Exception("错误：保费不能低于最低保费; ")

        if policymodel.trafficType == "" or policymodel.trafficType.lower() == "null":
            raise Exception("错误：TransportModeCode必须传递且不能为空; ")
        # else:
        #     if policymodel.trafficType == "1":
        #         policymodel.trafficType = "水运"
        #     elif policymodel.trafficType == "2":
        #         policymodel.trafficType = "铁路"
        #     elif policymodel.trafficType == "3":
        #         policymodel.trafficType = "汽运"
        #     elif policymodel.trafficType == "4":
        #         policymodel.trafficType = "空运"
        #     elif policymodel.trafficType == "8":
        #         policymodel.trafficType = "水运"
        #     else:
        #         raise Exception("错误：TransportModeCode无法识别; ")
        if policymodel.departDateTime == "" or policymodel.departDateTime.lower() == "null":
            raise Exception("错误：InsureDateTime必须传递且不能为空; ")
        else:
            #20170526 153733
            if len(policymodel.departDateTime) != 14:
                raise Exception("错误：InsureDateTime格式有误，正确格式：20170526153733; ")
            else:
                departDateTimes = policymodel.departDateTime
                policymodel.departDateTime = departDateTimes[0:4] + "-" + departDateTimes[4:6] + "-" + departDateTimes[6:8] + " "+ departDateTimes[8:10] + ":" + departDateTimes[10:12]+ ":" + departDateTimes[12:14]

         # 如果对接众安，需要校验省市区三段式的有效性（参照单票投保）
        
        if policymodel.departProvince == "" and departCity == "" and departDistrict == "" and detailDeparture == "":
            raise Exception("错误：起运地详细地址或省市区至少有一项不为空; ")
        if PlaceOrLocationInformation[1]['PlaceProvince'] == "" and destinationCity == "" and destinationDistrict == "" and detailDestination == "":
            raise Exception("错误：目的地详细地址或省市区至少有一项不为空; ")
        maxData = "select MAX(policySolutionID) from  remotedata WHERE policyNo='" + policymodel.policyNo + "' AND appkey='" + remoteuser + "'"
        # print(maxData)
        dataresult = dal.SQLHelper.fetch_one(maxData)
        # print(len(dataresult))
        if len(dataresult) == 0:
            newPolicyNo = policymodel.policyNo + "00000001"
        else:
            if (dataresult[0][0] is None):
                newPolicyNo = policymodel.policyNo + "00000001"
            else:
                aaa = policymodel.policyNo[6:]
                newPolicyNo = "COPSHH" + str(int(aaa) + 1)
        policymodel.policySolutionID = newPolicyNo

        policymodel.save()
        result = {}
        result['responsecode'] = '1'
        result['responsemessage'] = '投保成功'
        result['applicationserial'] = '投保成功'
        resultReturn = json.dumps(result)
        return json.loads(resultReturn)
    except Exception as err:
        result = {}
        result['responsecode'] = '0' 
        # print(err)
        traceback.print_exc()
        result['responsemessage'] = str(err)
        result['applicationserial'] = ''
        resultReturn = json.dumps(result)
        # 发送报错邮件
        # sendAlertMail('qian.hong@dragonins.com,manman.zhang@dragonins.com','德坤投递出错','客户原始报文:'+str(err)+str(postdata))
        sendAlertMail('manman.zhang@dragonins.com','德坤投递出错',str(err)+ '<br />' +str(postdata))
        #写入日志
        log_file = 'E:/policyinterface2/logs/'+datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")+'dekun.log'
        log_format = '%(message)s'
        logging.basicConfig(filename=log_file,format=log_format)
        logger = logging.getLogger()
        logger.warning(str(err) + str(postdata))

        return json.loads(resultReturn)

# 注销接口
@app.route('/cancelpolicy/<appkey>/<billno>', methods=['GET'])
def cancelpolicy(appkey, billno):
    from dals import dal
    try:
        sql = "SELECT guid FROM remotedata WHERE appkey='%s' AND shipId='%s'" %(appkey, billno)
        dataResult = dal.SQLHelper.fetch_one(sql)
        if len(dataResult) == 0:
            raise Exception("无法找到您要注销的运单")
        sql = "UPDATE remotedata SET Status='已注销' WHERE appkey='%s' AND shipId='%s'" %(appkey, billno)
        update(sql)
        cancelResult = {}
        cancelResult['result'] = 1
        cancelResult['retmsg'] = '注销成功'
        result = json.dumps(cancelResult)
        return json.loads(result)
    except Exception as err:
        cancelResult = {}
        cancelResult['result'] = 0
        cancelResult['retmsg'] = '注销失败: '+ str(err)
        result = json.dumps(cancelResult)
        return json.loads(result)


# 运单查询
@app.route('/getpolicy/<appkey>/<billno>', methods=['GET'])
def getpolicy(appkey, billno):
    try:
        remotedata = policy_model.remotedata.query.filter(policy_model.remotedata.appkey==appkey, policy_model.remotedata.shipId==billno).order_by(policy_model.remotedata.CreateDate.desc()).all()
        result = []
        dataresult = model_to_dict(remotedata)
        print(dataresult)
        if len(dataresult) == 0:
            raise Exception('无法找到您要查询的运单')
        policyresult = {}
        policyresult['appkey'] = appkey
        policyresult['billno'] = billno
        policyresult['sequencecode'] = dataresult[0]['guid']
        policyresult['status'] = dataresult[0]['Status']
        policyresult['policyid'] = dataresult[0]['policySolutionID']
        policyresult['policydownloadurl'] = ''
        searchResult = {}
        searchResult['result'] = 1
        searchResult['retmsg'] = ''
        searchResult['data'] = policyresult
        result = json.dumps(searchResult)
        return json.loads(result)
    except Exception as err:
        searchResult = {}
        searchResult['result'] = 0
        searchResult['retmsg'] = '查询失败: '+str(err)
        searchResult['data'] = {}
        result = json.dumps(searchResult)
        return json.loads(result)


# 聚盟结果查询
@app.route('/getjmpolicy/<appkey>/<billno>', methods=['GET'])
def getjmpolicy(appkey, billno):
    from models import jm_ht_policy_model
    channelOrderId = ""
    try:
        remotedata = jm_ht_policy_model.jm_ht_remotedata.query.filter(jm_ht_policy_model.jm_ht_remotedata.appkey==appkey, jm_ht_policy_model.jm_ht_remotedata.shipId==billno,jm_ht_policy_model.jm_ht_remotedata.claimLimit!='LK999999').order_by(jm_ht_policy_model.jm_ht_remotedata.CreateDate.desc()).all()
        dataresult = model_to_dict(remotedata)
        result = {}       
        if len(dataresult) == 0:
            raise Exception('无法找到您要查询的运单')
        else:
            status = dataresult[0]['Status']
            if status == "投保成功":
                responsemessage = '投保成功'
                result['responsecode'] = 1
                result['responsemessage'] = responsemessage
            elif status == "投保失败":
                responsemessage = "投保失败"
                result['responsecode'] = 0
                result['responsemessage'] = dataresult[0]['errLog']
            else:
                responsemessage = "等待投保"
                result['responsecode'] = -1
                result['responsemessage'] = responsemessage

        channelOrderId = dataresult[0]['channelOrderId']    
        result['applicationserial'] = str(dataresult[0]['guid'])
        result['appkey'] = appkey
        result['sequencecode'] = channelOrderId
        result['premium'] = dataresult[0]['insuranceFee']
        result['policyno'] = dataresult[0]['policySolutionID']
        result['downloadurl'] = dataresult[0]['relationType']
        return json.dumps(result)
    except Exception as err:
        traceback.print_exc()
        result = {}
        result['responsecode'] = 0
        result['responsemessage'] = str(err)
        result['applicationserial'] = ''
        result['appkey'] = appkey
        result['sequencecode'] = channelOrderId
        result['premium'] = ''
        result['policyno'] = ''
        result['downloadurl'] = ''
        return json.dumps(result)


# 投递保险公司(华泰)
def postInsurer_HT(guid):
    from models import RBProductInfo_model
    from models import InsurerSpec_model
    from models import ValidInsured_model
    from models import jm_ht_policy_model as policy_model
    from models import GJXXPT_Product_model
    from dals import dal
    try:
        #region 读取等待投保数据
        remotedata = policy_model.jm_ht_remotedata.query.filter(policy_model.jm_ht_remotedata.guid==guid).all()
        remotedata = model_to_dict(remotedata)
        appkey = remotedata[0]['appkey']
        productNo = remotedata[0]['claimLimit']
        custCoName = remotedata[0]['custCoName'] 
        ######公共信息General
        issueTime=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S").replace(" ", "T") #出单时间
        insurancePolicy = "" #保单号
        serialNumber=guid.replace("-","") #流水号
        #产品校验
        GJXXPTProduct=GJXXPT_Product_model.GJXXPTProduct.query.filter(GJXXPT_Product_model.GJXXPTProduct.appkey==appkey, GJXXPT_Product_model.GJXXPTProduct.InsuranceCoverageCode==productNo).all()
        GJXXPTProduct = model_to_dict(GJXXPTProduct)
        if len(GJXXPTProduct)==0 :
            raise Exception('产品配置信息不存在，投保失败')
        RBProductInfo=RBProductInfo_model.RBProductInfo.query.filter(RBProductInfo_model.RBProductInfo.line_no==GJXXPTProduct[0]['numCode']).all()
        RBProductInfo = model_to_dict(RBProductInfo)
        if len(RBProductInfo)==0 :
            raise Exception('分产品配置信息不存在，投保失败')
        #产品校验
        insuranceCode=RBProductInfo[0]['MainGlausesCode'] #险种代码
        insuranceName=RBProductInfo[0]['MainGlauses'] #险种名称
        effectivTime=datetime.datetime.strptime(remotedata[0]['departDateTime'], "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d") #保险起期
        terminalTime=(datetime.datetime.strptime(remotedata[0]['departDateTime'], "%Y-%m-%d %H:%M:%S")+datetime.timedelta(days=30)).strftime("%Y-%m-%d") #保险止期
        copy="1" #份数
        signTM=datetime.datetime.now().strftime("%Y-%m-%d") #签单时间

        ######运输信息Freight
        sign="无" #货物标记 (默认国内传递空或者无) 
        packAndQuantity=remotedata[0]['packageType']+remotedata[0]['cargoWeight']+remotedata[0]['mpAmount'] #包装及数量
        fregihtItem=remotedata[0]['cargoName'] #货物项目
        invoiceNumber="" #发票号
        #billNumber="详见运单" #提单号
        billNumber = "订单号 "+remotedata[0]['shipId']
        freightType=GJXXPTProduct[0]['CargoTypeClassification1'] #货物类型（编码）
        freightDetail=GJXXPTProduct[0]['BXcargoCode'] #二级货物类型明细（编码）
        invoiceMoney=remotedata[0]['cargeValue'] #发票金额
        invoiceBonus="1" #加成 (国内默认1)
        amt=decimal.Decimal(invoiceMoney)*decimal.Decimal(invoiceBonus) #保险金额
        amtCurrency="01" #保险金额币种（编码）(国内默认人民币01)
        exchangeRate="1" #汇率 (国内默认1)
        rate=decimal.Decimal(GJXXPTProduct[0]['Rate'].split('%',1)[0])/100
        rate2=0
        if remotedata[0]['policyRate']!="":
            rate2=decimal.Decimal(remotedata[0]['policyRate'].split('%',1)[0])/100
        chargeRate=rate*1000
        if remotedata[0]['policyRate']== "":
            chargeRate=rate2*1000 #费率‰ 
        premium=remotedata[0]['insuranceFee'] #保险费
        premiumCurrency="01" #保险费币种（编码）(国内默认人民币01)
        premiumPrit=RBProductInfo[0]['AdditiveNo'] #保费打印
        transportType=GJXXPTProduct[0]['TransportModeCode'] #运输方式（编码）
        transportDetail=GJXXPTProduct[0]['BXcargoName'] #二级运输方式明细（编码）
        if remotedata[0]['licenseId']=="":
            trafficNumber = "订单号 "+ remotedata[0]['shipId']
        else:
            trafficNumber = "车牌号 "+ remotedata[0]['licenseId']
        flightsCheduled="" #航次
        buildYear="" #建造年份
        fromCountry="HTC01" #起运地国家（编码）

        ######起运地国家（编码）/起运地
        fromArea=remotedata[0]['departProvince']+remotedata[0]['departCity']+remotedata[0]['departDistrict'] + remotedata[0]['departSpot'] #起运地 
        passPort="" #途径港S
        toContry="HTC01" #目的地国家 （编码）

        ######目的地国家（编码）/目的地
        toArea=remotedata[0]['destinationProvice']+remotedata[0]['destinationCity']+remotedata[0]['destinationDistrict']+remotedata[0]['deliveryAddress'] #目的地  
        surveyAdrID=RBProductInfo[0]['Additive'] #查勘地址编码
        surveyAdr="" #查勘地址内容 
        trantsTool="" #转运工具
        startTM=str(datetime.datetime.strptime(remotedata[0]['departDateTime'], "%Y-%m-%d %H:%M:%S")).replace(" ", "T")
        endTM=str(datetime.datetime.strptime(remotedata[0]['departDateTime'], "%Y-%m-%d %H:%M:%S")+datetime.timedelta(days=30)).replace(" ", "T")
        originalSum="1" #正文份数 
        datePritType="1" #日期打印方式(编码）(国内为中文1)
        InsurerSpec=InsurerSpec_model.InsurerSpec.query.filter(InsurerSpec_model.InsurerSpec.numCode==RBProductInfo[0]['SpecialAnnounce']).all()
        InsurerSpec = model_to_dict(InsurerSpec)
        if len(InsurerSpec)==0 :
            raise Exception('特别约定配置信息不存在，投保失败')
        mark="" #特别约定 
        creditNO="" #信用证编码
        creditNODesc="" #信用证描述
        trailerNum="" #挂车车牌号
        payAddr="" #赔款偿付地

        ######险种信息InsureRdr，获取
        rdrCde=GJXXPTProduct[0]['InsuranceCode'] #编码 (国内)
        rdrName=GJXXPTProduct[0]['InsuranceCoverageName'] #名称 (国内)
        rdrDesc=GJXXPTProduct[0]['Remark'] #描述

        # 附加盗抢险，不进行配置，写死在程序里
        rdrCde1="SX400069" #编码 (国内)
        rdrName1="盗抢险条款" #名称 (国内)
        rdrDesc1="盗抢险条款" #描述

        ######投保人信息Applicant
        appCode="" #投保人编码
        applicantName=remotedata[0]['custCoName'] #投保人名称
        gender="" #投保人性别
        birthday="" #投保人生日
        IDType="99" #证件类别（传固定值99开票传97） 
        ID="" #证件号码 
        phone="" #固定电话 
        cell="" #联系手机
        zip="" #邮政编码
        address="" #地址 
        email="" #Email
        taxDeduct="1" #是否要增值税发票 
        accountBank="" #开户银行 
        accountNumber="" #银行账号
        if taxDeduct=="1":
            ValidInsured=ValidInsured_model.ValidInsured.query.filter(ValidInsured_model.ValidInsured.Appkey==appkey,ValidInsured_model.ValidInsured.ValidInsuredName==custCoName).all()
            ValidInsured = model_to_dict(ValidInsured)
            if len(ValidInsured)==0 :
                raise Exception('开票信息配置信息不存在，投保失败')
            accountBank=ValidInsured[0]['CertfCls']
            accountNumber=ValidInsured[0]['ClntMrk']
            IDType="97"
            ID=ValidInsured[0]['CertfCde']
            phone=ValidInsured[0]['Tel']
            address=ValidInsured[0]['DetailAddress']

        #####被保险人信息Insured
        insuredName=remotedata[0]['insuredName'] #被保险人名称
        insuredGender="" #被保险人性别
        insuredBirthday="" #被保险人生日
        insuredIDType="99" #证件类别
        insuredID="不详" #证件号码 
        insuredPhone="" #固定电话
        insuredCell="" #联系手机
        insuredZip="" #邮政编码
        insuredAddress="" #地址
        insuredEmail="" #Email
        #endregion

        usr = "U030000328"  
        pwd = "fffe578ef8bba01e80dae7e17457cade"
        url="http://202.108.103.154:8080/HT_interfacePlatform/webservice/ImportService?wsdl" # 测试地址
        key = "1qaz2wsx" # 测试key
        # 通过产品号区分投递环境 
        if productNo=="LK999999":
            url="http://202.108.103.154:8080/HT_interfacePlatform/webservice/ImportService?wsdl"
            key = "1qaz2wsx" # 测试key
        elif productNo=="LK046001":
            url = "http://219.141.242.74:9081/HT_interfacePlatform/webservice/ImportService?wsdl" # 生产地址
            key = "2wsx1qaz" # 生产key
        else:
            raise Exception("产品编号【"+productNo+"】尚未开通投保")

        if url == "":
            raise Exception("投保系统尚未开通")

        client=Client(url)#Client里面直接放访问的URL，可以生成一个webservice对象
        postXML = """<?xml version='1.0' encoding='utf-8'?>
                        <Policy>
                            <General>
                                <IssueTime>"""+str(issueTime)+"""</IssueTime>
                                <SerialNumber>"""+str(serialNumber)+"""</SerialNumber>
                                <InsurancePolicy>"""+str(insurancePolicy)+"""</InsurancePolicy>
                                <InsuranceCode>"""+str(insuranceCode)+"""</InsuranceCode>
                                <InsuranceName>"""+str(insuranceName)+"""</InsuranceName>
                                <EffectivTime>"""+str(effectivTime)+"""</EffectivTime>
                                <TerminalTime>"""+str(terminalTime)+"""</TerminalTime>
                                <Copy>"""+str(copy)+"""</Copy>
                                <SignTM>"""+str(signTM)+"""</SignTM>
                            </General>
                            <Freight>
                                <Sign>"""+str(sign)+"""</Sign>
                                <PackAndQuantity>"""+str(packAndQuantity)+"""</PackAndQuantity>
                                <FregihtItem>"""+str(fregihtItem)+"""</FregihtItem>
                                <InvoiceNumber>"""+str(invoiceNumber)+"""</InvoiceNumber>
                                <BillNumber>"""+str(billNumber)+"""</BillNumber>
                                <FreightType>"""+str(freightType)+"""</FreightType>
                                <FreightDetail>"""+str(freightDetail)+"""</FreightDetail>
                                <InvoiceMoney>"""+str(invoiceMoney)+"""</InvoiceMoney>
                                <InvoiceBonus>"""+str(invoiceBonus)+"""</InvoiceBonus>
                                <Amt>"""+str(amt)+"""</Amt>
                                <AmtCurrency>"""+str(amtCurrency)+"""</AmtCurrency>
                                <ExchangeRate>"""+str(exchangeRate)+"""</ExchangeRate>
                                <ChargeRate>"""+str(chargeRate)+"""</ChargeRate>
                                <Premium>"""+str(premium)+"""</Premium>
                                <PremiumCurrency>"""+str(premiumCurrency)+"""</PremiumCurrency>
                                <PremiumPrit>"""+str(premiumPrit)+"""</PremiumPrit>
                                <TransportType>"""+str(transportType)+"""</TransportType>
                                <TransportDetail>"""+str(transportDetail)+"""</TransportDetail>
                                <TrafficNumber>"""+str(trafficNumber)+"""</TrafficNumber>
                                <FlightsCheduled>"""+str(flightsCheduled)+"""</FlightsCheduled>
                                <BuildYear>"""+str(buildYear)+"""</BuildYear>
                                <FromCountry>"""+str(fromCountry)+"""</FromCountry>
                                <FromArea>"""+str(fromArea)+"""</FromArea>
                                <PassPort>"""+str(passPort)+"""</PassPort>
                                <ToContry>"""+str(toContry)+"""</ToContry>
                                <ToArea>"""+str(toArea)+"""</ToArea>
                                <SurveyAdrID>"""+str(surveyAdrID)+"""</SurveyAdrID>
                                <SurveyAdr>"""+str(surveyAdr)+"""</SurveyAdr>
                                <TrantsTool>"""+str(trantsTool)+"""</TrantsTool>
                                <StartTM>"""+str(startTM)+"""</StartTM>
                                <EndTM>"""+str(endTM)+"""</EndTM>
                                <OriginalSum>"""+str(originalSum)+"""</OriginalSum>
                                <DatePritType>"""+str(datePritType)+"""</DatePritType>
                                <Mark>"""+str(mark)+"""</Mark>
                                <CreditNO>"""+str(creditNO)+"""</CreditNO>
                                <CreditNODesc>"""+str(creditNODesc)+"""</CreditNODesc>
                                <TrailerNum>"""+str(trailerNum)+"""</TrailerNum>
                                <PayAddr>"""+str(payAddr)+"""</PayAddr>
                            </Freight>
                            <InsureRdrs>
                                <InsureRdr>
                                    <RdrCde>"""+str(rdrCde)+"""</RdrCde>
                                    <RdrName>"""+str(rdrName)+"""</RdrName>
                                    <RdrDesc>"""+str(rdrDesc)+"""</RdrDesc>
                                </InsureRdr>
                                <InsureRdr>
                                    <RdrCde>"""+str(rdrCde1)+"""</RdrCde>
                                    <RdrName>"""+str(rdrName1)+"""</RdrName>
                                    <RdrDesc>"""+str(rdrDesc1)+"""</RdrDesc>
                                </InsureRdr>
                            </InsureRdrs>
                            <Applicant>
                                <AppCode>"""+str(appCode)+"""</AppCode>
                                <ApplicantName>"""+str(applicantName)+"""</ApplicantName>
                                <Gender>"""+str(gender)+"""</Gender>
                                <Birthday>"""+str(birthday)+"""</Birthday>
                                <IDType>"""+str(IDType)+"""</IDType>
                                <ID>"""+str(ID)+"""</ID>
                                <Phone>"""+str(phone)+"""</Phone>
                                <Cell>"""+str(cell)+"""</Cell>
                                <Zip>"""+str(zip)+"""</Zip>
                                <Address>"""+str(address)+"""</Address>
                                <Email>"""+str(email)+"""</Email>
                                <TaxDeduct>"""+str(taxDeduct)+"""</TaxDeduct>
                                <AccountBank>"""+str(accountBank)+"""</AccountBank>
                                <AccountNumber>"""+str(accountNumber)+"""</AccountNumber>
                            </Applicant>
                            <Insured>
                                <InsuredName>"""+str(insuredName)+"""</InsuredName>
                                <Gender>"""+str(insuredGender)+"""</Gender>
                                <Birthday>"""+str(insuredBirthday)+"""</Birthday>
                                <IDType>"""+str(insuredIDType)+"""</IDType>
                                <ID>"""+str(insuredID)+"""</ID>
                                <Phone>"""+str(insuredPhone)+"""</Phone>
                                <Cell>"""+str(insuredCell)+"""</Cell>
                                <Zip>"""+str(insuredZip)+"""</Zip>
                                <Address>"""+str(insuredAddress)+"""</Address>
                                <Email>"""+str(insuredEmail)+"""</Email>
                            </Insured>
                        </Policy>"""

        m = hashlib.md5()
        b = (key + postXML).encode(encoding='utf-8')
        m.update(b)
        signmd5 = m.hexdigest()
        result = client.service.IMPPolicy(postXML, usr, pwd, signmd5.upper())

        #写入日志
        log_file = open('logs/' + guid +'_jmpolicy.log',mode='a')
        log_file.write('---------------------------发给华泰报文 ' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '---------------------------\n')
        log_file.write("usr:" + usr + '\n')
        log_file.write("pwd:" + pwd + '\n')
        log_file.write("key:" + key + '\n')
        log_file.write("url:" + url + '\n')
        log_file.write(postXML + '\n')
        log_file.write('---------------------------对接华泰结果 ' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '---------------------------\n')
        log_file.write(str(result))
        log_file.close()

        from xml.dom.minidom import parse
        import xml.dom.minidom
        DOMTree = xml.dom.minidom.parseString(str(result))
        resultNode = DOMTree.documentElement

        _Msg = ""
        _SerialNumber = ""
        _InsurancePolicy = ""
        _PdfURL = ""
        _Status = ""
        
        _Flag = resultNode.getElementsByTagName("Flag")[0].childNodes[0].data
        if _Flag == "2": # 人工核保
            _Msg = resultNode.getElementsByTagName("Msg")[0].childNodes[0].data
            _SerialNumber = resultNode.getElementsByTagName("SerialNumber")[0].childNodes[0].data
            _InsurancePolicy = resultNode.getElementsByTagName("InsurancePolicy")[0].childNodes[0].data
            _Status = "人工核保"
        elif _Flag == "1": # 核保通过
            _SerialNumber = resultNode.getElementsByTagName("SerialNumber")[0].childNodes[0].data
            _InsurancePolicy = resultNode.getElementsByTagName("InsurancePolicy")[0].childNodes[0].data
            _PdfURL = resultNode.getElementsByTagName("PdfURL")[0].childNodes[0].data    
            _Status = "投保成功"
            if productNo == "LK999999":
                _Status = '测试数据'
        else: # 投保失败
            _Msg = resultNode.getElementsByTagName("Msg")[0].childNodes[0].data
            _SerialNumber = resultNode.getElementsByTagName("SerialNumber")[0].childNodes[0].data
            _InsurancePolicy = resultNode.getElementsByTagName("InsurancePolicy")[0].childNodes[0].data
            _PdfURL = resultNode.getElementsByTagName("PdfURL")[0].childNodes[0].data
            _Status = "投保失败"

        # 回写投保表
        sql = "UPDATE jm_ht_remotedata SET Status='%s', errLog='%s', policySolutionID='%s', relationType='%s' WHERE guid='%s'" %(_Status, _Msg, _InsurancePolicy, _PdfURL, guid)
        dal.SQLHelper.update(sql,None)
        return _Status, _InsurancePolicy, _PdfURL, _Msg, _Flag
    except Exception as err:
        traceback.print_exc()
        return "投保失败", "", "", str(err), "0"


# 投保接口 (弘胜)
@app.route('/hspolicy', methods=['POST'])
def hspolicy():
    cust_sequencecode = ''
    cust_appkey = ''
    from models import hs_ht_policy_model as policy_model
    from models import GJXXPT_Product_model
    from dals import dal
    from models import ValidInsured_model
    postdata = ""
    try:
        # 获取请求 
        postdata = json.loads(request.get_data(as_text=True))
        policymodel = policy_model.hs_ht_remotedata()
        
        newguid = str(uuid.uuid1())
        #写入日志
        log_file = open('logs/' + newguid +'_hspolicy.log',mode='a')
        log_file.write('---------------------------收到客户报文 ' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '---------------------------\n')
        log_file.write(str(postdata) + '\n')
        log_file.close()
        # 保存客户运单
        policymodel.guid = newguid        
        # 头部信息
        policymodel.appkey = postdata['appkey']     
        cust_appkey = postdata['appkey']
        policymodel.bizContent = postdata['usercode']
        policymodel.channelOrderId = postdata['sequencecode']
        cust_sequencecode = postdata['sequencecode']
        policymodel.policyNo = postdata['solutionid']
        policymodel.claimLimit = postdata['productid']       

        action = postdata['action']  
        policymodel.Status = '等待投保'
        policymodel.CreateDate = datetime.datetime.now()
        # 投保主体
        policymodel.custCoName = postdata['applicantname']
        policymodel.custProperty = postdata['applicanttype']
        policymodel.custId = postdata['applicantidnumber']
        policymodel.insuredName = postdata['insuredname']
        policymodel.shipperProperty = postdata['insuredtype']        
        policymodel.shipperId = postdata['insuredidnumber']
        policymodel.shipperName = postdata['spname']

        # 保险信息
        policymodel.cargeValue = postdata['policyamount']
        policymodel.policyRate = postdata['rate']
        policymodel.termContent = postdata['deductible']
        policymodel.insuranceFee = postdata['premium']
        policymodel.mpObject = postdata['insurancecoveragename']
        policymodel.mpRelation = postdata['chargetypecode']
        policymodel.departDateTime = postdata['insuredatetime']
        policymodel.shipId = postdata['originaldocumentnumber']
        policymodel.trafficType = postdata['transportmodecode']
        policymodel.licenseId = postdata['vehiclenumber']
        policymodel.departProvince = postdata['startprovince']
        policymodel.departCity = postdata['startcity']
        policymodel.departDistrict = postdata['startdistrict']
        policymodel.destinationProvice = postdata['endprovince']
        policymodel.destinationCity = postdata['endcity']
        policymodel.destinationDistrict = postdata['enddistrict']
        policymodel.departSpot = postdata['startaddress']
        policymodel.deliveryAddress = postdata['endaddress']
        policymodel.departStation = postdata['startareacode']
        policymodel.arriveStation = postdata['endareacode']
        policymodel.arriveProperty = postdata['transitaddress']
        policymodel.cargoName = postdata['descriptionofgoods']
        policymodel.cargoType = postdata['cargotype']
        policymodel.packageType = postdata['packagetype']
        policymodel.cargoCount = postdata['packagequantity']
        policymodel.cargoKind = postdata['packageunit']
        policymodel.cargoWeight = postdata['weight']
        policymodel.mpAmount = postdata['weightunit']
        policymodel.volume = postdata['volume']
        policymodel.mpRate = postdata['volumeunit']

        ValidInsured = ValidInsured_model.ValidInsured.query.filter(ValidInsured_model.ValidInsured.Appkey==policymodel.appkey,ValidInsured_model.ValidInsured.ValidInsuredName==policymodel.custCoName).all()
        ValidInsured = model_to_dict(ValidInsured)
        if len(ValidInsured)==0 :
            raise Exception('开票信息配置信息不存在，投保失败')
        #必填项校验
        exMessage = ''
        
        if policymodel.appkey == "":
            exMessage += "appkey不能为空;"
        if policymodel.bizContent == "":
            exMessage += "usercode不能为空;"
        if policymodel.channelOrderId == "":
            exMessage += "sequencecode不能为空;"
        if policymodel.claimLimit == "":
            exMessage += "productid不能为空;"
        if action == "":
            exMessage += "action不能为空;"
        if policymodel.custCoName == "":
            exMessage += "applicantname不能为空;"  
        if policymodel.custProperty == "":
            exMessage += "applicanttype不能为空;"
        if policymodel.custId == "":
            exMessage += "applicantidnumber不能为空;"
        if policymodel.insuredName == "":
            exMessage += "insuredName不能为空;"
        if policymodel.shipperProperty == "":
            exMessage += "insuredtype不能为空;"
        if policymodel.shipperId == "":
            exMessage += "insuredidnumber不能为空;"
        if policymodel.shipperName == "":
            exMessage += "spname不能为空;"
        if policymodel.cargeValue == "":
            exMessage += "policyamount不能为空;"
        if policymodel.policyRate == "":
            exMessage += "rate不能为空;"
        if policymodel.termContent == "":
            exMessage += "deductible不能为空;"
        if policymodel.insuranceFee == "":
            exMessage += "premium不能为空;"
        if policymodel.mpObject == "":
            exMessage += "insurancecoveragename不能为空;"
        if policymodel.mpRelation == "":
            exMessage += "chargetypecode不能为空;"
        if policymodel.departDateTime == "":
            exMessage += "insuredatetime不能为空;"
        else:        
            if len(policymodel.departDateTime) != 14:
                raise Exception("错误：起运日期departDateTime格式有误，正确格式：20170526153733;")
            else:                   
                # 倒签单校验
                departDateTimes = policymodel.departDateTime
                policymodel.departDateTime = str(departDateTimes[0:4]) + "-" + str(departDateTimes[4:6]) + "-" + str(departDateTimes[6:8]) + " "+ str(departDateTimes[8:10]) + ":" + str(departDateTimes[10:12])+ ":" + str(departDateTimes[12:14])
                time = int(departDateTimes)+10000 # 加10000相当于一个小时
                now = int(datetime.datetime.now().strftime("%Y%m%d%H%M%S"))
                
                if time - now < 100: # 100相当于小于1分钟
                    exMessage += "当前不允许倒签单;"

        if policymodel.departProvince == "":
            exMessage += "startprovince不能为空;"
        if policymodel.departCity == "":
            exMessage += "startcity不能为空;"
        if policymodel.departDistrict == "":
            exMessage += "startdistrict不能为空;"
        if policymodel.destinationProvice == "":
            exMessage += "endprovince不能为空;"
        if policymodel.destinationCity == "":
            exMessage += "endcity不能为空;"
        if policymodel.destinationDistrict == "":
            exMessage += "enddistrict不能为空;"
        if policymodel.departSpot == "":
            exMessage += "startaddress不能为空;"
        if policymodel.deliveryAddress == "":
            exMessage += "endaddress不能为空;"      
        if policymodel.shipId == "" and policymodel.licenseId =="":
            exMessage += "运单号或者车牌号至少一个必填;"
        if policymodel.trafficType == "":
            exMessage += "transportmodecode不能为空;"
        if policymodel.cargoName == "":
            exMessage += "descriptionofgoods不能为空;"
        if policymodel.cargoType == "":
            exMessage += "cargotype不能为空;"
        if policymodel.cargoCount == "":
            exMessage += "packagequantity不能为空;"
        if exMessage !="":
            raise Exception(exMessage)

        #单据唯一性
        remotedata = policy_model.hs_ht_remotedata.query.filter(policy_model.hs_ht_remotedata.appkey==postdata['appkey'], policy_model.hs_ht_remotedata.Status=='投保成功', policy_model.hs_ht_remotedata.channelOrderId==postdata['sequencecode']).order_by(policy_model.hs_ht_remotedata.CreateDate.desc()).all()
        result = []
        dataresult = model_to_dict(remotedata)
        if postdata['action'] == "apply":
            if len(dataresult) > 0:   
                raise Exception("sequencecode已存在重复,请不要重复投递")


        # 产品信息校验
        productdata = GJXXPT_Product_model.GJXXPTProduct.query.filter(GJXXPT_Product_model.GJXXPTProduct.appkey == postdata['appkey'],
        GJXXPT_Product_model.GJXXPTProduct.InsuranceCoverageCode == postdata['productid'])
        dataresult = model_to_dict(productdata)
        if len(dataresult) !=1:
            raise Exception("产品未配置!")
        product_deductible = dataresult[0]['deductible']
        product_insurancecoveragename = dataresult[0]['InsuranceCoverageName']
        product_transportcode = dataresult[0]['TransportModeCode']
        product_chargetypecode = dataresult[0]['ChargeTypeCode']
        product_cargotype = dataresult[0]['CargoTypeClassification1']
        product_lowestpremium = dataresult[0]['MonetaryAmount']#最低保费
        product_rate = dataresult[0]['Rate'] #约定费率
        product_maxAmount = dataresult[0]['PolicyAmount'] #最高保额

        if policymodel.termContent == "按约定":
            policymodel.termContent = product_deductible
        if policymodel.mpObject == "按约定":
            policymodel.mpObject = product_insurancecoveragename
        if policymodel.trafficType == "按约定":
            policymodel.trafficType = product_transportcode
        if policymodel.mpRelation == "按约定":
            policymodel.mpRelation = product_chargetypecode
        if policymodel.policyRate == "按约定":
            policymodel.policyRate = product_rate
        if policymodel.cargoType == "按约定":
            policymodel.cargoType = product_cargotype

        if policymodel.termContent != product_deductible:
            raise Exception("免赔额与产品定义不符")
        if policymodel.mpObject != product_insurancecoveragename:
            raise Exception("险别名称与产品定义不符")
        if policymodel.trafficType != product_transportcode:
            raise Exception("运输方式编码与产品定义不符")
        if policymodel.mpRelation != product_chargetypecode:
            raise Exception("计费方式与产品定义不符")        
        if policymodel.cargoType != product_cargotype:
            raise Exception("货物类型编码与产品定义不符")

        # 保费校验 如果触发最低保费，则不校验费率，否则需校验

        if product_maxAmount != "" and float(product_maxAmount) != 0.00:
            # 触发最高保额          
            m3 = decimal.Decimal(str(decimal.Decimal(policymodel.cargeValue))) - decimal.Decimal(str(decimal.Decimal(product_maxAmount)))
            if m3 > 0 :
                raise Exception("超过与保险公司约定的最高保额",product_maxAmount)
            # 如果触发最低保费，则不校验费率，否则需校验
        if decimal.Decimal(str(decimal.Decimal(policymodel.insuranceFee))) >= decimal.Decimal(str(decimal.Decimal(product_lowestpremium))):
            # 保费=保额*费率
            _rate=decimal.Decimal(product_rate.split('%',1)[0])/100
            _premium = decimal.Decimal((decimal.Decimal(policymodel.cargeValue) * _rate))          
            if decimal.Decimal(str(decimal.Decimal(policymodel.insuranceFee))) !=_premium:
                raise Exception("保费计算有误")
        else:
            raise Exception("保费不能低于合同约定的最低保费")#触发最低保费

        policymodel.save()

        # 投递保险公司 或 龙琨编号
        """_Status, _InsurancePolicy, _PdfURL, _Msg, _Flag = postInsurer_HT(newguid)       
        if _Flag == "1":
            _Msg = "投保成功"
        result = {}
        result['responsecode'] = _Flag
        result['responsemessage'] = _Msg
        result['applicationserial'] = newguid
        result['appkey'] = policymodel.appkey
        result['sequencecode'] = policymodel.channelOrderId
        result['premium'] = policymodel.insuranceFee
        result['policyno'] = _InsurancePolicy
        result['downloadurl'] = _PdfURL
        resultReturn = json.dumps(result)
        return json.loads(resultReturn)"""
        return "投保成功"
    
    except Exception as err:
        traceback.print_exc()
        result = {}
        result['responsecode'] = '0'
        result['responsemessage'] = str(err)
        result['applicationserial'] = ''
        result['appkey'] = cust_appkey
        result['sequencecode'] = cust_sequencecode
        result['premium'] = ''
        result['policyno'] = ''
        result['downloadurl'] = ''
        log_file = open('logs/' + newguid +'_hspolicy.log',mode='a')
        log_file.write('---------------------------弘胜报错信息 ' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '---------------------------\n')
        log_file.write(str(err))
        log_file.close()
        sendAlertMail('manman.zhang@dragonins.com','弘胜—龙琨投递出错',str(err)+'<br />' + str(postdata))
        resultReturn = json.dumps(result)
        return json.loads(resultReturn)


# 发送注册验证邮件
def sendAlertMail(mailaddr, mailtitle, mailcontent):
    sender = 'policy@dragonins.com'
    receivers = [mailaddr]  # 接收邮件，可设置为你的QQ邮箱或者其他邮箱
    # 三个参数：第一个为文本内容，第二个 plain 设置文本格式，第三个 utf-8 设置编码
    message = MIMEText(mailcontent, 'html', 'utf-8')
    message['Subject'] = Header(mailtitle, 'utf-8')

    try:
        mail_host = 'smtp.exmail.qq.com'  # 设置服务器
        mail_user = 'policy@dragonins.com'    # 用户名
        mail_pass = '7rus7U5!'   # 口令
        smtpObj = smtplib.SMTP()
        smtpObj.connect(mail_host, 25)    # 25 为 SMTP 端口号
        smtpObj.login(mail_user, mail_pass)
        smtpObj.sendmail(sender, receivers, message.as_string())
        print('邮件发送成功')
    except smtplib.SMTPException:
        print('Error: 无法发送邮件')


# 数据查询结果转dict字典
def model_to_dict(result):
    from collections.abc import Iterable
    # 转换完成后，删除  '_sa_instance_state' 特殊属性
    try:
        if isinstance(result, Iterable):
            tmp = [dict(zip(res.__dict__.keys(), res.__dict__.values())) for res in result]
            for t in tmp:
                t.pop('_sa_instance_state')
        else:
            tmp = dict(zip(result.__dict__.keys(), result.__dict__.values()))
            tmp.pop('_sa_instance_state')
        return tmp
    except BaseException as e:
        print(e.args)
        raise TypeError('Type error of parameter')


# 调试开关
# debug = True
# 运行
if(__name__ == '__main__'):
    app.run(host='0.0.0.0', port=5000, debug=True)

