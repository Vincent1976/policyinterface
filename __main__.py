import datetime
import json
import smtplib
import uuid
from email.header import Header
from email.mime.text import MIMEText
from flask import Flask, redirect, render_template, request
import commfunc
import config
from dals import dal
from database.exts import db
from models import policy_model
import traceback
import suds
from suds.client import Client
import hashlib


# 创建flask对象
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


# 具体执行sql语句的函数
def query(sql, params=None):
    result = dal.SQLHelper.fetch_all(sql, params)
    return result


# 具体执行sql语句的函数
def update(sql, params=None):
    result = dal.SQLHelper.update(sql, params)
    return result

# 首页路由
@app.route('/', methods=['POST'])
def index():
    return 'Welcome!'

# 投保接口
@app.route('/sendpolicy', methods=['POST'])
def sendpolicy():
    try:
        # 获取请求 
        postdata = json.loads(request.get_data(as_text=True))
       # 保存客户运单
        policymodel = policy_model.remotedata()
        policymodel.guid = str(uuid.uuid1())
        policymodel.channelOrderId = postdata['sequencecode']
        policymodel.Status = '等待投保'
        policymodel.CreateDate = datetime.datetime.now()
        policymodel.appkey = postdata['action']            
        policymodel.bizContent = postdata['usercode']
        policymodel.policyNo = postdata['solutionid']
        policymodel.policySolutionID = postdata['productid']
        policymodel.applicanttype = postdata['applicanttype']
        policymodel.custId = postdata['applicantidnumber']
        policymodel.insuredName = postdata['insuredname']
        policymodel.insuredtype = postdata['insuredtype']
        policymodel.shipperProperty = postdata['insuredidnumber']
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
        if policymodel.policySolutionID == "":
            exMessage += "productid不能为空;"
        if policymodel.appkey == "":
            exMessage += "action不能为空;"
        if policymodel.applicanttype == "":
            exMessage += "applicanttype不能为空;"
        if policymodel.custId == "":
            exMessage += "applicantidnumber不能为空;"
        if policymodel.insuredName == "":
            exMessage += "insuredName不能为空;"
        if policymodel.insuredtype == "":
            exMessage += "insuredtype不能为空;"
        if policymodel.shipperProperty == "":
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
        elif policymodel.departDateTime.len != 14:
           raise Exception("错误：起运日期InsureDateTime格式有误，正确格式：20170526153733;")
        

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
        if policymodel.cargoCount:
            exMessage += "packagequantity不能为空;"
        if exMessage !="":
            raise Exception(exMessage)
        print(exMessage)
            

    
        # 投递保险公司 或 龙琨编号
        # 反馈客户

        result = {}
        result['responsecode'] = '1'
        result['responsemessage'] = '投保成功'
        result['applicationserial'] = '投保成功'
        resultReturn = json.dumps(result)
        return json.loads(resultReturn)

    except Exception as err:
        result = {}
        result['responsecode'] = '0'
        result['responsemessage'] = str(err)
        result['applicationserial'] = ''
        resultReturn = json.dumps(result)
        return json.loads(resultReturn)+exMessage


# 注销接口
@app.route('/cancelpolicy/<appkey>/<billno>', methods=['GET'])
def cancelpolicy(appkey, billno):
    try:
        sql = "SELECT guid FROM remotedata WHERE appkey='%s' AND shipId='%s'" %(appkey, billno)
        dataResult = query(sql)
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


# 投递保险公司(华泰)
def postInsurer_HT(guid):
    try:
        url="http://202.108.103.154:8080/HT_interfacePlatform/webservice/ImportService?wsdl" #这里是你的webservice访问地址
        client=Client(url)#Client里面直接放访问的URL，可以生成一个webservice对象
        postXML = """<?xml version='1.0' encoding='utf-8'?>
                        <Policy>
                            <General>
                                <IssueTime>2020-06-11T23:00:43</IssueTime>
                                <SerialNumber>2b9035517b5944d5a80e0280e2fb3371</SerialNumber>
                                <InsurancePolicy></InsurancePolicy>
                                <InsuranceCode>3601</InsuranceCode>
                                <InsuranceName>国内水路、陆路货物运输保险</InsuranceName>
                                <EffectivTime>2020-06-11</EffectivTime>
                                <TerminalTime>2020-07-11</TerminalTime>
                                <Copy>1</Copy>
                                <SignTM>2020-06-11</SignTM>
                            </General>
                            <Freight>
                                <Sign>无</Sign>
                                <PackAndQuantity>纸盒,18吨</PackAndQuantity>
                                <FregihtItem>香蕉</FregihtItem>
                                <InvoiceNumber></InvoiceNumber>
                                <BillNumber>详见运单</BillNumber>
                                <FreightType>SX001402</FreightType>
                                <FreightDetail>SX00140007</FreightDetail>
                                <InvoiceMoney>60000</InvoiceMoney>
                                <InvoiceBonus>1</InvoiceBonus>
                                <Amt>60000</Amt>
                                <AmtCurrency>01</AmtCurrency>
                                <ExchangeRate>1</ExchangeRate>
                                <ChargeRate>0.45000</ChargeRate>
                                <Premium>27.00</Premium>
                                <PremiumCurrency>01</PremiumCurrency>
                                <PremiumPrit>02</PremiumPrit>
                                <TransportType>SX001506</TransportType>
                                <TransportDetail>13</TransportDetail>
                                <TrafficNumber>赣C1075D</TrafficNumber>
                                <FlightsCheduled></FlightsCheduled>
                                <BuildYear></BuildYear>
                                <FromCountry>HTC01</FromCountry>
                                <FromArea>海南省三亚市</FromArea>
                                <PassPort></PassPort>
                                <ToContry>HTC01</ToContry>
                                <ToArea>江西省南昌市</ToArea>
                                <SurveyAdrID>501422495708</SurveyAdrID>
                                <SurveyAdr></SurveyAdr>
                                <TrantsTool></TrantsTool>
                                <StartTM>2020-06-11T22:55:00</StartTM>
                                <EndTM>2020-07-11T22:55:00</EndTM>
                                <OriginalSum>1</OriginalSum>
                                <DatePritType>1</DatePritType>
                                <Mark></Mark>
                                <CreditNO></CreditNO>
                                <CreditNODesc></CreditNODesc>
                                <TrailerNum></TrailerNum>
                                <PayAddr></PayAddr>
                            </Freight>
                            <InsureRdrs>
                                <InsureRdr>
                                    <RdrCde>SX300211</RdrCde>
                                    <RdrName>基本险</RdrName>
                                    <RdrDesc>国内水路、陆路货物运输保险基本险</RdrDesc>
                                </InsureRdr>
                                <InsureRdr>
                                    <RdrCde>SX400069</RdrCde>
                                    <RdrName>盗抢险条款</RdrName>
                                    <RdrDesc>盗抢险条款</RdrDesc>
                                </InsureRdr>
                            </InsureRdrs>
                            <Applicant>
                                <AppCode></AppCode>
                                <ApplicantName>江西零浩网络科技有限公司</ApplicantName>
                                <Gender></Gender>
                                <Birthday></Birthday>
                                <IDType>97</IDType>
                                <ID>91360121MA35KJA01N</ID>
                                <Phone>15279188388</Phone>
                                <Cell></Cell>
                                <Zip></Zip>
                                <Address>江西省南昌市南昌县莲塘镇莲安路158号百合佳苑住宅区2栋一单元401室</Address>
                                <Email></Email>
                                <TaxDeduct>1</TaxDeduct>
                                <AccountBank>建设银行南昌支行</AccountBank>
                                <AccountNumber>6236682020002708516</AccountNumber>
                            </Applicant>
                            <Insured>
                                <InsuredName>陈小将</InsuredName>
                                <Gender></Gender>
                                <Birthday></Birthday>
                                <IDType>99</IDType>
                                <ID>不详</ID>
                                <Phone></Phone>
                                <Cell></Cell>
                                <Zip></Zip>
                                <Address></Address>
                                <Email></Email>
                            </Insured>
                        </Policy>"""
        
        Usr = "ZTSQ-LTH"
        Pwd = "ac86a441509773a126cf531f2bf88fa5"
        m = hashlib.md5()
        b = ("2Wsx1Qaz" + postXML).encode(encoding='utf-8')
        m.update(b)
        SignMD5 = m.hexdigest()

        result = client.service.IMPPolicy(postXML, Usr, Pwd, SignMD5.upper())
        print(result)
        return 'success'
    except Exception as err:
        traceback.print_exc()
        return str(err)


# 发送注册验证邮件
def sendAlertMail(mailaddr, mailtitle, mailcontent):
    sender = 'policy@dragonins.com'
    receivers = [mailaddr]  # 接收邮件，可设置为你的QQ邮箱或者其他邮箱
    # 三个参数：第一个为文本内容，第二个 plain 设置文本格式，第三个 utf-8 设置编码
    message = MIMEText(mailcontent, 'plain', 'utf-8')
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
