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
import datetime
from models import GJXXPT_Product_model
from models import RBProductInfo_model
from models import InsurerSpec_model
from models import ValidInsured_model
import decimal


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
@app.route('/', methods=['GET'])
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
        policymodel.appkey = postdata['appkey']            
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
        else:        
            if len(policymodel.departDateTime) != 14:
                raise Exception("错误：起运日期departDateTime格式有误，正确格式：20170526153733;")
            else:
                departDateTimes = policymodel.departDateTime 
                policymodel.departDateTime = departDateTimes[0:4] + "-" + departDateTimes[4:6] + "-" + departDateTimes[6:8] + " "+ departDateTimes[8:10] + ":" + departDateTimes[10:12] + ":" + departDateTimes[12:14]

                # 倒签单校验
                # print(datetime.datetime.now().strftime("%Y%m%d%H%M%S"))
                # 获取当前时间多加一天 并转换为20200617104122(14位)
                # print((datetime.datetime.now()+datetime.timedelta(hours=1)).strftime("%Y%m%d%H%M%S"))
                # minutes = (policymodel.departDateTime - (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))).seconds*60
                # print(minutes)
                # m1 = policymodel.departDateTime
                # m2 = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                # print((datetime.datetime.now()+datetime.timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S"))
                # if datetime.datetime.strptime(policymodel.departDateTime).datetime.timedelta(hours=1).Subtract(datetime.datetime.Now()).TotalMinutes < 0:
                #     exMessage += "当前不允许倒签单;"

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
        if policymodel.departProvince != "" and policymodel.destinationProvice != "":
            return  
        #三段模式，需校验  
        elif policymodel.departStation != "" and policymodel.arriveStation != "":
        #地区码模式，需校验
            isDQM = True
            isSDS = False
        else:  
            return     
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
        # if isDQM == True:




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
        print(err)
        result['responsemessage'] = str(err)
        result['applicationserial'] = ''
        resultReturn = json.dumps(result)
        return json.loads(resultReturn)


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

# 洪仟测试
@app.route('/postInsurer_HT/<guid>/<appkey>', methods=['GET'])
def getpolicyTest(guid, appkey):
    return postInsurer_HT(guid, appkey)

# 投递保险公司(华泰)
def postInsurer_HT(guid,appkey):
    try:
        #region 读取等待投保数据
        remotedata = policy_model.remotedata.query.filter(policy_model.remotedata.guid==guid).all()
        remotedata = model_to_dict(remotedata)

        ######公共信息General
        issueTime=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S").replace(" ", "T") #出单时间
        insurancePolicy = "" #保单号
        serialNumber=guid.replace("-","") #流水号
        #产品校验
        GJXXPTProduct=GJXXPT_Product_model.GJXXPTProduct.query.filter(GJXXPT_Product_model.GJXXPTProduct.appkey==appkey).all()
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
        terminalTime=(datetime.datetime.strptime(remotedata[0]['departDateTime'], "%Y-%m-%d %H:%M:%S")+datetime.timedelta(days=2)).strftime("%Y-%m-%d") #保险止期
        copy="1" #份数
        signTM=datetime.datetime.now().strftime("%Y-%m-%d") #签单时间

        ######运输信息Freight
        sign="无" #货物标记 (默认国内传递空或者无) 
        packAndQuantity=remotedata[0]['packageType']+remotedata[0]['cargoWeight']+"吨" #包装及数量
        fregihtItem=remotedata[0]['cargoName'] #货物项目
        invoiceNumber="" #发票号
        billNumber="详见运单" #提单号
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
        trafficNumber=remotedata[0]['licenseId'] #保险费
        flightsCheduled="" #航次
        buildYear="" #建造年份
        fromCountry="HTC01" #起运地国家（编码）

        ######起运地国家（编码）/起运地
        fromArea=remotedata[0]['departProvince']+remotedata[0]['departCity']+remotedata[0]['departDistrict'] #起运地 
        passPort="" #途径港S
        toContry="HTC01" #目的地国家 （编码）

        ######目的地国家（编码）/目的地
        toArea=remotedata[0]['destinationProvice']+remotedata[0]['destinationCity']+remotedata[0]['destinationDistrict'] #目的地  
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

        ######险种信息InsureRdr
        rdrCde="SX300211" #编码 (国内)
        rdrName="基本险" #名称 (国内)
        rdrDesc="国内水路、陆路货物运输保险基本险" #描述
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
            ValidInsured=ValidInsured_model.ValidInsured.query.filter(ValidInsured_model.ValidInsured.Appkey==appkey).all()
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

        url="http://202.108.103.154:8080/HT_interfacePlatform/webservice/ImportService?wsdl" #这里是你的webservice访问地址
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
        
        #写入日志
        f1 = open('/logs/'+datetime.datetime.now().strftime("%Y-%m-%d")+'sendpolicyHT.log','a')
        f1.write(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ", Request XML" + str(postXML))
        f1.close()

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
