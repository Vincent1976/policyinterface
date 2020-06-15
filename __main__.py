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

        #写入日志
        f1 = open('/tmp/sendpolicy.txt','a')
        f1.write(d_now_yyyy_mm_dd_HH_MM_SS(datetime.datetime.now()) + ", Request json：" + str(json.loads(postdata)))
        f1.close()

        #region 接收请求参数
        appkey=xstr(postdata['appkey'])
        usercode=xstr(postdata['usercode'])
        sequencecode=xstr(postdata['sequencecode'])
        solutionid=xstr(postdata['solutionid'])
        productid=xstr(postdata['productid'])
        action=xstr(postdata['action'])
        applicantname=xstr(postdata['applicantname'])
        applicanttype=xstr(postdata['applicanttype'])
        applicantidnumber=xstr(postdata['applicantidnumber'])
        insuredname=xstr(postdata['insuredname'])
        insuredtype=xstr(postdata['insuredtype'])
        insuredidnumber=xstr(postdata['insuredidnumber'])
        spname=xstr(postdata['spname'])
        policyamount=xstr(postdata['policyamount'])
        rate=xstr(postdata['rate'])
        deductible=xstr(postdata['deductible'])
        premium=xstr(postdata['premium'])
        insurancecoveragename=xstr(postdata['insurancecoveragename'])
        chargetypecode=xstr(postdata['chargetypecode'])
        insuredatetime=xstr(postdata['insuredatetime'])
        originaldocumentnumber=xstr(postdata['originaldocumentnumber'])
        transportmodecode=xstr(postdata['transportmodecode'])
        vehiclenumber=xstr(postdata['vehiclenumber'])
        startprovince=xstr(postdata['startprovince'])
        startcity=xstr(postdata['startcity'])
        startdistrict=xstr(postdata['startdistrict'])
        endprovince=xstr(postdata['endprovince'])
        endcity=xstr(postdata['endcity'])
        enddistrict=xstr(postdata['enddistrict'])
        startaddress=xstr(postdata['startaddress'])
        endaddress=xstr(postdata['endaddress'])
        startareacode=xstr(postdata['startareacode'])
        endareacode=xstr(postdata['endareacode'])
        transitaddress=xstr(postdata['transitaddress'])
        descriptionofgoods=xstr(postdata['descriptionofgoods'])
        cargotype=xstr(postdata['cargotype'])
        packagetype=xstr(postdata['packagetype'])
        packagequantity=xstr(postdata['packagequantity'])
        packageunit=xstr(postdata['packageunit'])
        weight=xstr(postdata['weight'])
        weightunit=xstr(postdata['weightunit'])
        volume=xstr(postdata['volume'])
        volumeunit=xstr(postdata['volumeunit'])
        #endregion

        #region 校验
        connStr=""
        if(len(postdata)>0)
        {
            #region 必填项校验
            exMessage = "";
            if (appkey) == "")
            {
                exMessage += "appkey不能为空;";
            }
            if (usercode == "")
            {
                exMessage += "usercode不能为空;";
            }
            if (usercode == "")
            {
                exMessage += "solutionid不能为空;";
            }
            if (sequencecode == "")
            {
                exMessage += "sequencecode不能为空;";
            }
            if (productid == "")
            {
                exMessage += "productid不能为空;";
            }
            if (action == "")
            {
                exMessage += "action不能为空;";
            }
            if (applicantname == "")
            {
                exMessage += "applicantname不能为空;";
            }
            if (applicanttype == "")
            {
                exMessage += "applicanttype不能为空;";
            }
            if (applicantidnumber == "")
            {
                exMessage += "applicantidnumber不能为空;";
            }
            if (insuredname == "")
            {
                exMessage += "insuredname不能为空;";
            }
            if (insuredtype == "")
            {
                exMessage += "insuredtype不能为空;";
            }
            if (insuredidnumber == "")
            {
                exMessage += "insuredidnumber不能为空;";
            }
            if (policyamount == "")
            {
                exMessage += "policyamount不能为空;";
            }
            if (rate == "")
            {
                exMessage += "rate不能为空;";
            }
            if (deductible == "")
            {
                exMessage += "deductible不能为空;";
            }
            if (premium == "")
            {
                exMessage += "premium不能为空;";
            }
            if (insurancecoveragename == "")
            {
                exMessage += "insurancecoveragename不能为空;";
            }
            if (chargetypecode == "")
            {
                exMessage += "chargetypecode不能为空;";
            }
            if (insuredatetime == "")
            {
                exMessage += "insuredatetime不能为空;";
            }
            else
            {
                if (insuredatetime.Length != 14)
                {
                    throw new Exception("错误：起运日期InsureDateTime格式有误，正确格式：20170526153733; ");
                }
                else
                {
                    departDateTime = insuredatetime;
                    insuredatetime = departDateTime.Substring(0, 4) + "-" + departDateTime.Substring(4, 2) + "-" + departDateTime.Substring(6, 2) + " "
                        + departDateTime.Substring(8, 2) + ":" + departDateTime.Substring(10, 2) + ":" + departDateTime.Substring(12, 2);
                    #倒签单校验
                    if (DateTime.Parse(loginkPolicyRequest.insuredatetime).AddHours(1).Subtract(DateTime.Now).TotalMinutes < 0)
                        exMessage += "当前不允许倒签单;";
                }
            }
            if (DATAPROCESS.NULL2STR(loginkPolicyRequest.originaldocumentnumber) + DATAPROCESS.NULL2STR(loginkPolicyRequest.vehiclenumber) == "")
            {
                exMessage += "运单号或者车牌号至少一个必填;";
            }
            if (DATAPROCESS.NULL2STR(loginkPolicyRequest.transportmodecode) == "")
            {
                exMessage += "transportmodecode不能为空;";
            }
            if (DATAPROCESS.NULL2STR(loginkPolicyRequest.startaddress) == "")
            {
                exMessage += "startaddress不能为空;";
            }
            if (DATAPROCESS.NULL2STR(loginkPolicyRequest.endaddress) == "")
            {
                exMessage += "endaddress不能为空;";
            }
            if (DATAPROCESS.NULL2STR(loginkPolicyRequest.descriptionofgoods) == "")
            {
                exMessage += "descriptionofgoods不能为空;";
            }
            if (DATAPROCESS.NULL2STR(loginkPolicyRequest.cargotype) == "")
            {
                exMessage += "cargotype不能为空;;";
            }

            if (DATAPROCESS.NULL2STR(loginkPolicyRequest.packagequantity) == "")
            {
                exMessage += "packagequantity不能为空;";
            }

            if (exMessage != "")
                throw new Exception(exMessage);

            bool isSDS = true;
            bool isDQM = false;
            if (startprovince != "" && endprovince != "")
            {
                #三段模式，需校验
            }
            else if (startcode != "" && endcode != "")
            {
                #地区码模式，需校验
                isDQM = true;
                isSDS = false;
            }
            else
            {
                #既不是三段模式，也不是地区码模式，需报错
                throw new Exception("地址信息有误，请确认三段模式或地区码模式");
            }
            #endregion
        }
        #endregion

        #region 保存客户运单
        #识别信息
        policymodel = policy_model.remotedata()
        policymodel.guid = str(uuid.uuid1())
        policymodel.appkey = userAppkey  #?
        policymodel.bizContent = usercode
        policymodel.shipperContact = spname
        policymodel.systemOrderId = sequencecode
        policymodel.policyNo = solutionid
        policymodel.relationType = productid
        policymodel.channelOrderId = sequencecode
        #投保主体
        policymodel.custCoName = applicantname
        applicanttype = applicanttype == "175" ? "企业" : "个人"
        policymodel.custProperty = applicanttype #?
        policymodel.custId = applicantidnumber
        policymodel.insuredName = insuredname
        insuredtype = applicanttype == "175" ? "企业" : "个人"
        policymodel.shipperProperty = insuredtype #?
        policymodel.shipperId = insuredidnumber
        #保险信息
        policymodel.cargeValue = policyamount
        policymodel.policyRate = rate
        policymodel.termContent = deductible
        policymodel.insuranceFee = premium
        policymodel.mpObject = insurancecoveragename
        policymodel.mpRelation = chargetypecode
        #运输信息
        policymodel.departDateTime = insuredatetime
        policymodel.transitSpot = originaldocumentnumber
        string trafficType = "公路"
        if (transportmodecode == "3")
        trafficType = "公路"
        else if (transportmodecode == "5")
        trafficType = "铁路"
        else if (transportmodecode == "8")
        trafficType = "水路"
        else
        trafficType = "公路"
        policymodel.trafficType = trafficType #?
        policymodel.licenseId = vehiclenumber

        policymodel.departProvince = startprovince #?
        policymodel.departCity = startcity #?
        policymodel.departDistrict = startdistrict #?
        policymodel.destinationProvice = endprovince #?
        policymodel.destinationCity = endcity #?
        policymodel.destinationDistrict = enddistrict #?
        policymodel.departStation = startareacode
        policymodel.arriveStation = endareacode
        policymodel.departSpot = startaddress
        policymodel.deliveryAddress = endaddress
        policymodel.arriveProperty = transitaddress
        policymodel.claimLimit2 = needpdf
        #货物信息
        policymodel.cargoName = descriptionofgoods
        policymodel.cargoType = cargotype
        policymodel.packageType = packagetype
        policymodel.cargoCount = packagequantity

        policymodel.cargoKind = packageunit
        policymodel.cargoWeight = weight
        policymodel.mpAmount = weightunit
        policymodel.volume = volume
        policymodel.mpRate = volumeunit

        policymodel.CreateDate = d_now_yyyy_mm_dd_HH_MM_SS(datetime.datetime.now())
        policymodel.Status = "等待投保"
   
        policymodel.save()        
        #endregion

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
        result['responsemessage'] = str(err).
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
def getpolicy(guid, appkey):
    postInsurer_HT(guid, appkey)

# 投递保险公司(华泰)
def postInsurer_HT(guid,appkey):
    try:
        # #region 读取等待投保数据
        # remotedata = policy_model.remotedata.query.filter(policy_model.remotedata.guid==guid).all()
        # issueTime=d_now_yyyy_mm_dd_HH_MM_SS(datetime.datetime.now()).replace(" ", "T") #出单时间
        # insurancePolicy = "" #保单号
        # serialNumber=guid.replace("-","") #流水号
        # #产品校验
        # GJXXPT_Product=GJXXPT_Product_model.GJXXPT_Product.query.filter(GJXXPT_Product_model.GJXXPT_Product.appkey==appkey).all()
        # if GJXXPT_Product==null
        #     raise Exception('产品配置信息不存在，投保失败')
        # RBProductInfo=RBProductInfo_model.RBProductInfo.query.filter(RBProductInfo_model.RBProductInfo.line_no==GJXXPT_Product['line_no']).all()
        # if RBProductInfo==null
        #     raise Exception('分产品配置信息不存在，投保失败')
        # #endregion
        # insuranceCode=rsRBProductInfo[0] #险种代码
        # insuranceName=rsRBProductInfo[1] #险种名称
        # effectivTime=d_now_yyyy_mm_dd(policyresult['departDateTime']) #保险起期
        # terminalTime=(datetime.date(d_now_yyyy_mm_dd(policyresult['departDateTime']))+datetime.timedelta(days=2)).strftime("%Y-%m-%d") #保险止期
        # copy="1" #份数
        # signTM=datetime.datetime.now().strftime("%Y-%m-%d") #签单时间
        # sign="无" #货物标记 (默认国内传递空或者无) 
        # packAndQuantity=policyresult['packageType']+policyresult['cargoWeight']+"吨" #包装及数量
        # fregihtItem=policyresult['cargoName'] #货物项目
        # invoiceNumber="" #发票号
        # billNumber="详见运单" #提单号
        # freightType=rsGJXXPTProduct[1] #货物类型（编码）
        # freightDetail=rsGJXXPTProduct[2] #二级货物类型明细（编码）
        # invoiceMoney=policyresult['cargeValue'] #发票金额
        # invoiceBonus="1" #加成 (国内默认1)
        # amt=Decimal(invoiceMoney)*Decimal(invoiceBonus) #保险金额
        # amtCurrency="01" #保险金额币种（编码）(国内默认人民币01)
        # exchangeRate="1" #汇率 (国内默认1)

        # #写入日志
        # f1 = open('/tmp/POST_TO_HT/logs/'+d_now_yyyymmdd(datetime.datetime.now())+'sendpolicy.txt','a')
        # f1.write(d_now_yyyy_mm_dd_HH_MM_SS(datetime.datetime.now()) + ", Request json：" + str(json.loads(postdata)))
        # f1.close()

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
