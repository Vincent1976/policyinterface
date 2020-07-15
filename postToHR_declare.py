import json
import pymssql
import requests
import traceback
import datetime
from dateutil.relativedelta import relativedelta
import decimal
import config
from dals import dal
import hashlib
import smtplib
from email.header import Header
from email.mime.text import MIMEText
from urllib import parse



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
# 华泰出单接口
def issueInterface(_proposalNo,guid):
    try:
        issuedata={}
        insureObject = {} # 投保信息
        insureObject["bizCode"]= '122' # 交易类型
        insureObject["proposalNo"]= _proposalNo # 投保单号
        insureObject["createTime"]= (datetime.datetime.now()).strftime("%Y-%m-%d %H:%M:%S") # 请求时间

        payObject = {} # 支付信息
        payObject["isSinglePay"] = '1' # 是否逐单支付
        payObject["payMode"] = '' # 支付类型
        payObject["payDate"] = '' # 支付时间
        payObject["payBankNo"] = '' # 支付流水号
                
        issuedata["insureObject"] = insureObject
        issuedata["payObject"] = payObject
        issueJson = json.dumps(issuedata)

        key1 = "1qaz2wsx" # 线下提供的密钥
        m1 = hashlib.md5()
        b1 = (key1 + issueJson).encode(encoding='utf-8')
        m1.update(b1)
        issuemd5 = m1.hexdigest()
                
        #post出单接口请求
        url="http://219.141.242.74:9039/service_platform/GeneralInsureInterface"
        # 通过字典方式定义请求body
        FormData = {"json": str(issueJson), "channelCode": str(channelObject["channelCode"]), "signature": str(issuemd5)}
        data = parse.urlencode(FormData)
        headers = {
            'Content-Type': "application/x-www-form-urlencoded",
        }
        # 请求方式
        content = requests.post(url=url, headers=headers, data=data).text
        content = json.loads(content)

        _bizCode1 = ""
        _channelCode1 = ""
        _orderId1 = ""
        _policyNO1 = ""
        _policyURL1 = ""
        _responseInfo1 = ""
        _totalPremium1 = ""
        _responseCode1 = content['responseCode'] # 接收返回的参数
        error = '报错信息：' + str(content)

        if _responseCode1 == "1":
            _bizCode1 = content['bizCode'] 
            _channelCode1 = content['channelCode'] 
            _policyNO1 = content['policyNO'] 
            _policyURL1 = content['policyURL'] 
            _orderId1 = content['orderId'] 
            _responseInfo1 = content['responseInfo'] 
            _totalPremium1 = content['totalPremium']
            _Status1 = "已出单" 
        elif _responseCode1 == "0":
            _bizCode1 = content['bizCode'] 
            _channelCode1 = content['channelCode'] 
            _policyNO1 = content['policyNO'] 
            _policyURL1 = content['policyURL'] 
            _orderId1 = content['orderId'] 
            _responseInfo1 = content['responseInfo'] 
            _totalPremium1 = content['totalPremium']
            _Status1 = "出单失败"
            sendAlertMail('manman.zhang@dragonins.com','钱江-对接华泰',str(guid) + '<br />' + str(error))
        else:
            _bizCode1 = content['bizCode'] 
            _channelCode1 = content['channelCode'] 
            _policyNO1 = content['policyNO'] 
            _policyURL1 = content['policyURL'] 
            _orderId1 = content['orderId'] 
            _responseInfo1 = content['responseInfo'] 
            _totalPremium1 = content['totalPremium']
            sendAlertMail('manman.zhang@dragonins.com','钱江-对接华泰',str(guid) + '<br />' + str(error))

        # 第二次回写remotedata投保表
        sql = "UPDATE remotedata SET relationType='%s', Status='%s',deliveryOrderId = '%s',errlog = '%s'  WHERE guid='%s'" %(_policyURL1, _Status1, _proposalNo,_responseInfo1, guid)
        dal.SQLHelper.update(sql,None)
    except Exception as err:
        traceback.print_exc()
        print("请求失败",err) 
            
try:
    # 打开数据库连接
    conn = pymssql.connect(host="121.36.193.132",port = "15343",user="sa",password="sate1llite",database="insurance",charset='utf8')
    cursor = conn.cursor() #创建一个游标对象，python 里的sql 语句都要通过cursor 来执行
    # sql = "select top (1)* from RemoteData left join ValidInsured on RemoteData.appkey = ValidInsured.Appkey where RemoteData.appkey='4a33b1fe29333104b90859253f4d1b68' and RemoteData.status = '等待投保' order by CreateDate desc  "   
    sql = "select top (1)* from RemoteData left join ValidInsured on RemoteData.appkey = ValidInsured.Appkey where RemoteData.appkey='4a33b1fe29333104b90859253f4d1b68'  order by CreateDate desc  "   

    cursor.execute(sql)   #执行sql语句
    data = cursor.fetchall()  #读取查询结果
    cursor.close()
    conn.close()

    for row in data: 
        postdata={}
        guid = row[0] 
        channelObject = {}
        channelObject["bizCode"]= '121' # 交易类型
        channelObject["channelCode"]='UE03009302' # 渠道编码
        channelObject["channelName"]='上海励琨互联网科技有限公司' # 渠道名称
        channelObject["orderId"]= row[14] # 订单号 shipid
        channelObject["createTime"]= str(datetime.datetime.now()) # 当前时间

        insuranceObject = {}
        insuranceObject["insuranceCode"] = '3622' # 险种代码
        insuranceObject["insuranceName"] = '承运人公路货运责任保险条款 ' # 产品名称
        insuranceObject['plan'] = '综合险' # 款别
        insuranceObject['srcCPlyNo'] = '' # 不必填
        insuranceObject['prmCur'] = '01' 
        insuranceObject['premium'] = row[21] # 保险费
        insuranceObject['amtCur'] = '01'
        insuranceObject['amount'] = '12000.0' 
        insuranceObject['rate'] = str(decimal.Decimal(row[68][:-1]) * 10) # policyRate 去除百分号后乘以10 [:-1] 截取从头开始到倒数第一个字符之前
        insuranceObject['effectiveTime'] = row[36]# 保险起期 departDateTime
        insuranceObject['terminalTime'] = str(datetime.datetime.strptime(insuranceObject['effectiveTime'],'%Y-%m-%d %H:%M:%S')+ datetime.timedelta(days = 15)) # 上面时间+15天
        insuranceObject['copy'] = '1' # 份数 
        insuranceObject['docType'] = '' # 不必填
        insuranceObject['docSN'] = '' # 不必填

        # 需要提供开票的六项信息
        appntObject = {}
        appntObject["appName"] = row[3] # 投保人姓名 custCoName
        appntObject["appType"] = '2' 
        appntObject["appBirthday"] = '' # 不必填
        appntObject["appEmail"] = '' # 不必填
        appntObject["appGender"] = '' # 不必填
        appntObject["appIDType"] = '97' 
        appntObject["appNumber"] = '' # 不必填
        appntObject["appTelNumber"] = row[86] # 投保人电话号
        appntObject["appAddr"] = row[90]# 地址信息
        appntObject["appContact"] = row[78] # 联系人名字
        appntObject["isTaxInvoice"] = '1' 
        appntObject["taxCertifi"] = row[83] # 税务登记证号
        appntObject["depositBank"] = row[82] # 开户银行
        appntObject["bankAccount"] = row[81] # 银行账户

        insuredObject = {}
        insuredObject["insuredName"] = row[9] # 被保险人名称
        insuredObject["insuredType"] = '2' # 
        insuredObject["insuredBirthday"] = '' # 不必填
        insuredObject["insuredEmail"] = '' # 不必填
        insuredObject["InsuredGender"] = '' # 不必填
        insuredObject["insuredIDType"] = '06' # 被保人证件类型

        if insuredObject["insuredName"] == '杭州钱江物流有限公司':
            insuredObject["insuredNumber"] = '91330102727621887N'# 被保人证件号
        elif insuredObject["insuredName"] == '杭州汉盛物流有限公司':
            insuredObject["insuredNumber"] = '91330182322907190D'# 被保人证件号
        else:
            insuredObject["insuredNumber"] = '不详'# 被保人证件号

        insuredObject["insuredTelNumber"] = '不详' # 被保险人电话
        insuredObject["insuredAddress"] = '' # 不必填
        insuredObject["relationship"] = '' # 不必填
        insuredObject["isLegal"] = '1' # 

        # benefitDTOs 当 isLegal = 1时 此节点不用传
        # start-----------不传------------------
        # benefitDTOs= {}
        # benefitDTOs["benefitName"] = '' 
        # benefitDTOs["benefitRelation"] = '' 
        # benefitDTOs["benefitListNumber"] = '' 
        # benefitDTOs["share"] = '' 
        # benefitDTOs["benefitIDType"] = '' 
        # benefitDTOs["benefitNumber"] = '' 

        # benefitDTOs1= {}
        # benefitDTOs1["benefitName"] = '' 
        # benefitDTOs1["benefitRelation"] = '' 
        # benefitDTOs1["benefitListNumber"] = '' 
        # benefitDTOs1["share"] = '' 
        # benefitDTOs1["benefitIDType"] = '' 
        # benefitDTOs1["benefitNumber"] = '' 
        # insuredObject["benefitDTOs"] = [benefitDTOs1,benefitDTOs1]
        # end---------------------------------------------


        # start-------------------不传--------------------------
        # insuredObject1 = {}
        # insuredObject1["insuredName"] = row[9] # 被保险人名称
        # insuredObject1["insuredType"] = '' 
        # insuredObject1["insuredBirthday"] = '' 
        # insuredObject1["insuredEmail"] = ''
        # insuredObject1["InsuredGender"] = '' 
        # insuredObject1["insuredIDType"] = '' 
        # insuredObject1["insuredNumber"] = '' 
        # insuredObject1["insuredTelNumber"] = '' 
        # insuredObject1["insuredAddress"] = '' 
        # insuredObject1["relationship"] = '' 
        # insuredObject1["isLegal"] = '' 
        # insuredObject1["benefitDTOs"] = []
        # end---------------------------------------------

        # --------- 以下整段问华泰
        definedSafeObj = {}
        definedSafeObj["isDefinedSafe"] = '0' # 默认0，此节点不用传
        # start----暂时不传--------
        # rdrDTOs = {}
        # rdrDTOs["rdrCode"] = "" 
        # rdrDTOs["rdrName"] = ""
        # rdrDTOs["rdrAmount"] = ""
        # rdrDTOs["rdrPremium"] = ""
        # rdrDTOs["rdrDeductible"] = ""
        # rdrDTOs["rdrRemark"] = ""

        # rdrDTOs1 = {}
        # rdrDTOs1["rdrCode"] = "" 
        # rdrDTOs1["rdrName"] = ""
        # rdrDTOs1["rdrAmount"] = ""
        # rdrDTOs1["rdrPremium"] = ""
        # rdrDTOs1["rdrDeductible"] = ""
        # rdrDTOs1["rdrRemark"] = ""
        # definedSafeObj["rdrDTOs"] = [rdrDTOs,rdrDTOs1]
        
        agreementObject = {}
        agreementObject["policyDeductible"] = '1）针对一般事故：每一运输工具每次事故人民币5000元或损失金额的10%，以高者为准；2）针对火灾、爆炸及运输工具倾覆或追尾他车：每一运输工具每次事故人民币10000元或损失金额的20%，以高者为准。' # 免赔额/率
        agreementObject["policySpec"] =  '1）被保险人在运输过程中，由于盗窃造成货物的损失，依法应由被保险人承担赔偿责任的，保险人按本保险合同约定负责赔偿。2）对于裸装货物、二手货（旧货）、退货及返修货物，本保险仅承保基本险的风险；3）本保险不负责任何形式的仓储期间的损失，但运输过程中的临时仓储除外；4）承运车辆须具备合格驾驶证、行驶证及营运许可证，否则，保险人不负赔偿责任； 5） 保险人放弃对以下车辆的代位追偿：赣CB6506，赣CB2613，皖CA4152，皖AD3005，赣E66415，赣E43782，赣E66043，粤ACN608，粤ACY261，粤AAK050；但不放弃对任何其它第三方责任人追偿的权利；6）本保单扩展承保目的地为乌鲁木齐的货物。' # 特别约定
        # end---------------
        # -----------------------

        productDiffObject = {}

        productDiffObject["reMark"] = '' # 默认为空
        productDiffObject["vehicleNum"] = '*' 
        productDiffObject["vehicleModel"] = '*'
        productDiffObject["vehicleLen"] = '*'
        productDiffObject["vehicleFrameNum"] = '*' 
        productDiffObject["goodsName"] = row[27] # 货物名称 cargoName

        productDiffObject["goodsQuantity"] = row[37] # 货物数量 cargoCount
        productDiffObject["goodsPack"] = '08' # 包装方式
        productDiffObject["goodsValue"] = row[18] # 货物价值 cargeValue
        productDiffObject["transFrom"] = row[28]+row[29]+row[30] #  省、市、区（departProvince + departCity + departDistrict）
        productDiffObject["transDepot"] = '' # 不必填
        productDiffObject["transTo"] = row[42] # 目的地 deliveryAddress


        postdata["channelObject"] = channelObject
        postdata["insuranceObject"] = insuranceObject
        postdata["appntObject"] = appntObject
        # result["insuredObject"] = [insuredObject,insuredObject1]
        postdata["insuredObject"] = [insuredObject]
        postdata["definedSafeObj"] = definedSafeObj
        postdata["agreementObject"] = agreementObject
        postdata["productDiffObject"] = productDiffObject
        Json = json.dumps(postdata)
        # print(Json)
        key = "1qaz2wsx" # 线下提供的密钥
        m = hashlib.md5()
        b = (key + Json).encode(encoding='utf-8')
        m.update(b)
        signmd5 = m.hexdigest()
        parameter = 'json=' + str(Json) + '&channelCode='+ str(channelObject["channelCode"]) + '&signature=' + str(signmd5)

        #写入日志
        log_file = open('logs/' + datetime.datetime.now().strftime("%Y%m%d%H%M%S%f") +'_huatai.log',mode='a')
        log_file.write('---------------------------发给华泰报文 ' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '---------------------------\n')
        log_file.write('---------------------------对接华泰结果 ' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '---------------------------\n')
        log_file.write(str(Json))
        log_file.close()
        
        #post接口请求
        url="http://219.141.242.74:9039/service_platform/GeneralUnderInterface"
        # 通过字典方式定义请求body
        FormData = {"json": str(Json), "channelCode": str(channelObject["channelCode"]), "signature": str(signmd5)}
        data = parse.urlencode(FormData)
        headers = {
            'Content-Type': "application/x-www-form-urlencoded",
        }
        # 请求方式
        content = requests.post(url=url, headers=headers, data=data).text
        content = json.loads(content)
        # print(content)
        _bizCode = ""
        _channelCode = ""
        _orderId = ""
        _responseInfo = ""
        _proposalNo = ""
        _Status = ""
        _responseCode = content['responseCode'] # 接收返回的参数

        guid = 'guid：'+guid
        error = '报错信息：' + str(content)
        if _responseCode == "2": # 人工核保
            _bizCode = content['bizCode'] 
            _channelCode = content['channelCode'] 
            _orderId = content['orderId'] 
            _responseInfo = content['responseInfo'] 
            _proposalNo = content['proposalNo']
            _Status = "人工核保" 
            sendAlertMail('manman.zhang@dragonins.com','钱江-对接华泰',str(guid) + '<br />' + str(error))
        elif _responseCode == "1": # 核保通过
            _bizCode = content['bizCode'] 
            _channelCode = content['channelCode'] 
            _orderId = content['orderId'] 
            _responseInfo = content['responseInfo'] 
            _proposalNo = content['proposalNo']
            _Status = "投保成功" 
        else: # 投保失败
            _bizCode = content['bizCode'] 
            _channelCode = content['channelCode'] 
            _orderId = content['orderId'] 
            _responseInfo = content['responseInfo'] 
            _proposalNo = content['proposalNo']
            _Status = "投保失败" 
            issueInterface(_proposalNo,guid)

            sendAlertMail('manman.zhang@dragonins.com','钱江-对接华泰',str(guid) + '<br />' + str(error)) 
        # 第一次回写remotedata投保表
        sql = "UPDATE remotedata SET Status='%s', errLog='%s',bizContent = '%s',custid = '%s' WHERE guid='%s'" %(_Status, _responseInfo,_proposalNo, _orderId, guid)
        dal.SQLHelper.update(sql,None)

except Exception as err:
    traceback.print_exc()
    print("请求失败",err) 

