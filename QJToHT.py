import json
import pymssql
import requests
import traceback
import datetime
# from dateutil.relativedelta import relativedelta
import decimal
# from dals import dal
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
        mail_pass = 'Sate1llite'   # 口令
        smtpObj = smtplib.SMTP()
        smtpObj.connect(mail_host, 25)    # 25 为 SMTP 端口号
        smtpObj.login(mail_user, mail_pass)
        smtpObj.sendmail(sender, receivers, message.as_string())
        print('邮件发送成功')
    except smtplib.SMTPException:
        print('Error: 无法发送邮件')

# 华泰出单接口
def issueInterface():
    FormData = ''
    try:
        # 打开数据库连接
        conn = pymssql.connect(host="121.36.193.132",port = "15343",user="sa",password="sate1llite",database="insurance",charset='utf8')
        cursor = conn.cursor() #创建一个游标对象，python 里的sql 语句都要通过cursor 来执行
        sql = "select *, datediff(second,CreateDate,departDateTime) 'diff'  from RemoteData left join ValidInsured on RemoteData.appkey = ValidInsured.Appkey where RemoteData.appkey='4a33b1fe29333104b90859253f4d1b68' and RemoteData.status = '等待投保' order by CreateDate" 
        # sql = "select  * from RemoteData left join ValidInsured on RemoteData.appkey = ValidInsured.Appkey where RemoteData.appkey='4a33b1fe29333104b90859253f4d1b68' and RemoteData.guid = 'df08fb7d-0824-4191-87cd-bddedcf3dc76'  order by CreateDate desc" 

        cursor.execute(sql)   #执行sql语句
        data = cursor.fetchall()  #读取查询结果
        # cursor.close()
        # conn.close()
        for row in data:
            postdata={}
            guid = row[0]

            # 校验倒签
            diff=float(row[91])
            if diff < -1200:
                sql = "UPDATE remotedata SET Status = '投保失败', errLog = '起运日期不能早于投保日期' WHERE guid = '"+guid+"'"
                cursor.execute(sql)
                conn.commit()
                sendAlertMail('manman.zhang@dragonins.com','钱江——华泰投递出错','起运日期不能早于投保日期，guid=' + str(guid))
                continue

            channelObject = {}
            channelObject["bizCode"]= '101' # 交易类型
            channelObject["orderId"]= row[14] # 订单号 shipid
            channelObject["createTime"]= str(datetime.datetime.now())[0:19] # 当前时间
            insuranceObject = {}
            # 测试环境
            # channelObject["channelCode"]='100189' # 渠道编码  
            # key = "123456@HT" # 线下提供的密钥
            # channelObject["channelName"]='上海励琨互联网科技有限公司' # 渠道名称
            # insuranceObject["insuranceCode"] = '362205' # 险种代码
            # insuranceObject["insuranceName"] = '承运人公路货运责任保险条款 ' # 险种名称(产品名称)
            #post出单接口请求
            # url="http://219.141.242.74:9039/service_platform/InsureInterface"


            # 正式环境
            channelObject["channelCode"]='100189' # 渠道编码  
            key = "shlk2020@HT" # 线下提供的密钥
            channelObject["channelName"]='上海励琨' # 渠道名称
            insuranceObject["insuranceCode"] = '362205' # 险种代码
            insuranceObject["insuranceName"] = '上海励琨-钱江物流 ' # 险种名称(产品名称)
            url="http://219.141.242.74:9004/service_platform/InsureInterface"

            insuranceObject['plan'] = 'A' # 款别
            insuranceObject['srcCPlyNo'] = '' # 不必填
            insuranceObject['prmCur'] = '01' 

            # 保费为0时校验
            insuranceFee=float(row[21])
            if insuranceFee == 0.0:
                insuranceFee = 0.01

            insuranceObject['premium'] = insuranceFee # 保费       
            insuranceObject['amtCur'] = '01'
            insuranceObject['amount'] = row[18] # 保额
            insuranceObject['rate'] = str(decimal.Decimal(row[68][:-1]) * 10) # policyRate 去除百分号后乘以10 [:-1] 截取从头开始到倒数第一个字符之前
            insuranceObject['effectiveTime'] = row[36]# 保险起期 departDateTime
            insuranceObject['terminalTime'] = str(datetime.datetime.strptime(insuranceObject['effectiveTime'],'%Y-%m-%d %H:%M:%S')+ datetime.timedelta(days = 30)) # 上面时间+30天
            insuranceObject['copy'] = '1' # 份数 
            insuranceObject['docType'] = '' # 不必填
            insuranceObject['docSN'] = '' # 不必填

            # 需要提供开票的六项信息
            appntObject = {}
            appntObject["appName"] = row[3] # 投保人姓名 custCoName
            appntObject["appType"] = '2' 
            # appntObject["appBirthday"] = '' # 不必填
            appntObject["appEmail"] = '' # 不必填
            appntObject["appGender"] = '' # 不必填
            appntObject["appIDType"] = '97' 

            if appntObject["appName"] == '杭州钱江物流有限公司':
                appntObject["appNumber"] = '91330102727621887N'# 被保人证件号
            elif appntObject["appName"] == '杭州汉盛物流有限公司':
                appntObject["appNumber"] = '91330182322907190D'# 被保人证件号
            else:
                appntObject["appNumber"] = '不详'# 被保人证件号
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
            definedSafeObj = {}
            definedSafeObj["isDefinedSafe"] = '0' # 默认0，此节点不用传

            payObject = {} # 支付信息
            payObject["isSinglePay"] = '0' # 是否逐单支付
            # payObject["payMode"] = '' # 支付类型
            # payObject["payDate"] = '' # 支付时间
            # payObject["payBankNo"] = '' # 支付流水号
            
            agreementObject = {}
            agreementObject["policyDeductible"] = '1）针对一般事故：每一运输工具每次事故人民币5000元或损失金额的10%，以高者为准；2）针对火灾、爆炸及运输工具倾覆或追尾他车：每一运输工具每次事故人民币10000元或损失金额的20%，以高者为准。' # 免赔额/率
            agreementObject["policySpec"] =  "1）被保险人在运输过程中，由于盗窃造成货物的损失，依法应由被保险人承担赔偿责任的，保险人按本保险合同约定负责赔偿。2）对于裸装货物、二手货（旧货）、退货及返修货物，本保险仅承保基本险的风险；3）本保险不负责任何形式的仓储期间的损失，但运输过程中的临时仓储除外；4）承运车辆须具备合格驾驶证、行驶证及营运许可证，否则，保险人不负赔偿责任； 5） 保险人放弃对以下车辆的代位追偿：赣CB6506，赣CB2613，皖CA4152，皖AD3005，赣E66415，赣E43782，赣E66043，粤ACN608，粤ACY261，粤AAK050；但不放弃对任何其它第三方责任人追偿的权利；6）本保单扩展承保目的地为乌鲁木齐的货物。7）未尽事宜以协议（MC9033622202004001）为准" # 特别约定

            productDiffObject = {}

            productDiffObject["reMark"] = '' # 默认为空
            productDiffObject["vehicleNum"] = row[14] # 运单号 shipid
            productDiffObject["vehicleModel"] = '*'
            productDiffObject["vehicleLen"] = '*'
            productDiffObject["vehicleFrameNum"] = '*' 
            productDiffObject["goodsName"] = row[27] # 货物名称 cargoName
            productDiffObject["goodsType"] = 'SX001420' # 货物大类
            productDiffObject["goodsQuantity"] = row[37] # 货物数量 cargoCount
            productDiffObject["goodsPack"] = '08' # 包装方式
            productDiffObject["goodsValue"] = row[18] # 货物价值 cargeValue
            productDiffObject["transFrom"] = row[28]+row[29]+row[30] #  省、市、区（departProvince + departCity + departDistrict）
            productDiffObject["transDepot"] = row[46] # 中转地
            productDiffObject["transTo"] = row[31]+row[32]+row[33]+row[42] # 目的地 省、市、区（destinationProvice + destinationCity + destinationDistrict+deliveryAddress
            productDiffObject["transDate"] = row[36] # 起运日期
            productDiffObject["transportCost"] = row[18] # 运费

            postdata["channelObject"] = channelObject
            postdata["insuranceObject"] = insuranceObject
            postdata["appntObject"] = appntObject
            postdata["insuredObject"] = [insuredObject]
            postdata["definedSafeObj"] = definedSafeObj
            postdata['payObject'] = payObject
            postdata["agreementObject"] = agreementObject
            postdata["productDiffObject"] = productDiffObject
            Json = json.dumps(postdata, ensure_ascii=False)
            Json2 = Json.replace("%", "%25").replace("&", "%26").replace("+", "%2B")
            # print(Json)          
            m = hashlib.md5()
            b = (str(Json2) + key).encode(encoding='utf-8')
            m.update(b)
            signmd5 = m.hexdigest()
            # print(signmd5)

            #写入日志
            log_file = open('logs/' + datetime.datetime.now().strftime("%Y%m%d%H%M%S%f") +'_huatai.log',mode='a', encoding='utf-8')
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
            content = json.loads(content)
            log_file.write('---------------------------对接华泰结果 ' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '---------------------------\n')
            log_file.write(str(content))
            log_file.close()
            print(content)
            _bizCode = "" # 业务类型
            _responseCode = "" # 错误代码
            _responseInfo = "" # 错误信息
            _channelCode = "" # 渠道编码
            _orderId = "" # 订单号
            _totalPremium = "" # 总保费
            _policyNO = "" # 保单号
            _policyURL = "" # 电子保单地址
            _responseCode = content['responseCode'] # 接收返回的参数
            guiderr = 'guid：'+guid
            error = '报错信息：' + str(content)
            if _responseCode == "2": # 人工核保
                _bizCode = content['bizCode'] 
                _responseInfo = content['responseInfo'] 
                _Status = "人工核保" 
                sendAlertMail('manman.zhang@dragonins.com','钱江-对接华泰',str(guiderr) + '<br />' + str(error))
            elif _responseCode == "1": # 核保通过
                _bizCode = content['bizCode'] 
                _responseInfo = content['responseInfo'] 
                _channelCode = content['channelCode'] 
                _orderId = content['orderId'] 
                _totalPremium = content['totalPremium'] 
                _policyNO = content['policyNO'] 
                _policyURL = content['policyURL']
                _Status = "投保成功" 
            else: # 投保失败
                _bizCode = content['bizCode'] 
                _responseInfo = content['responseInfo'] 
                _Status = "投保失败" 
                sendAlertMail('manman.zhang@dragonins.com','钱江-对接华泰',str(guiderr) + '<br />' + str(error)) 
            # 回写remotedata投保表
            sql = "UPDATE remotedata SET Status = '"+_Status+"', errLog = '"+_responseInfo+"', bizContent = '"+_policyNO+"', relationType = '"+_policyURL+"'  WHERE guid = '"+guid+"'"
            cursor.execute(sql) #执行sql 语句
            conn.commit() #提交
    except Exception as err:
        traceback.print_exc()
        print("请求失败",err) 
        sendAlertMail('manman.zhang@dragonins.com','钱江——华泰投递出错',str(err)+'<br />' + str(FormData))

issueInterface() # 调用华泰出单接口

         
