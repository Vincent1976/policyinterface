import json
import pymssql
import requests
import traceback
import datetime
import decimal
import hashlib
import smtplib
from email.header import Header
from email.mime.text import MIMEText
from urllib import parse

# 发送注册验证邮件
def sendAlertMail(mailaddr, mailtitle, mailcontent):
    sender = 'policy@dragonins.com'
    receivers = mailaddr  # 接收邮件，可设置为你的QQ邮箱或者其他邮箱
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
def issueInterface():
    FormData=''

    try:
        # 打开数据库连接
        conn = pymssql.connect(host="121.36.193.132",port = "15343",user="sa",password="sate1llite",database="kahang",charset='utf8')
        cursor = conn.cursor() #创建一个游标对象，python 里的sql 语句都要通过cursor 来执行
        sql = "select *,datediff(second,CreateDate,departDateTime) 'diff' from RemoteData where RemoteData.appkey='03D9AC28-3AF5-488C-9F5A-2EDD41331F8A' and RemoteData.status = '等待投保' order by CreateDate desc" 

        cursor.execute(sql)   #执行sql语句
        data = cursor.fetchall()  #读取查询结果
        # cursor.close()
        # conn.close()
        print(data)

        for row in data:
            postdata={}
            guid = row[0] 

            # 校验倒签
            diff=float(row[76])
            if diff<-1200:
                sql = "UPDATE remotedata SET Status = '投保失败', errLog = '起运日期不能早于投保日期' WHERE guid = '"+guid+"'"
                cursor.execute(sql)
                conn.commit()
                sendAlertMail('qian.hong@dragonins.com,manman.zhang@dragonins.com','卡航投递华泰出错','起运日期不能早于投保日期，guid=' + str(guid))
                continue

            # 校验被保险人
            cursor.execute("select * from ValidInsured where ValidInsuredName='"+str(row[3])+"'")
            insuredData = cursor.fetchall()
            if len(insuredData)==0:
                raise Exception("被保险人"+str(row[3])+"未配置")

            channelObject = {}
            insuranceObject = {}

            ############################ 测试环境
            # channelObject["channelCode"]= '100192' # 渠道编码
            # key = "123456@HT" # 线下提供的密钥
            # channelObject["channelName"]='上海励琨互联网科技有限公司' # 渠道名称
            # insuranceObject["insuranceCode"] = '362206' # 险种代码
            # insuranceObject["insuranceName"] = '承运人公路货运责任保险条款 ' # 产品名称
            # url="http://219.141.242.74:9039/service_platform/InsureInterface"

            ############################ 正式环境
            channelObject["channelCode"]= '100192' # 渠道编码
            key = "shlkkh2020@HT" # 线下提供的密钥
            channelObject["channelName"]='上海励琨' # 渠道名称
            insuranceObject["insuranceCode"] = '362206' # 险种代码
            insuranceObject["insuranceName"] = '上海励琨-卡航 ' # 产品名称
            url="http://219.141.242.74:9004/service_platform/InsureInterface"


            channelObject["bizCode"]= '121' # 交易类型
            channelObject["orderId"]= row[14] # 订单号 shipid
            channelObject["createTime"]= str(datetime.datetime.now())[0:19] # 当前时间

            # 校验最低保费
            claimLimit= float(row[17])
            cargoFreight=float(row[16])
            insuranceFee=float(row[21])
            rate=str(decimal.Decimal(row[68][:-1]) * 10)
            plan=''
            policyDeductible=''
            rdrAmount=0
            if claimLimit==300000:
                rdrAmount=300000
                plan='A'
                policyDeductible='RMB1,000元或损失金额的5%,以高者为准'
                rate='1.5'
                insuranceFee=cargoFreight*0.0015
                if insuranceFee<=3.5:
                    insuranceFee=3.5
            elif claimLimit==2000000:
                rdrAmount=2000000
                plan='B'
                policyDeductible='RMB2,000元或损失金额的5%,以高者为准'
                rate='2'
                insuranceFee=cargoFreight*0.002
                if insuranceFee<=10:
                    insuranceFee=10
            elif claimLimit==5000000:
                rdrAmount=5000000
                plan='C'
                policyDeductible='RMB3,000元或损失金额的5%,以高者为准'
                rate='2.5'
                insuranceFee=cargoFreight*0.0025
                if insuranceFee<=25:
                    insuranceFee=25
            else:
                sql = "UPDATE remotedata SET Status = '投保失败', errLog = '赔款限额不存在' WHERE guid = '"+guid+"'"
                cursor.execute(sql) 
                conn.commit() 
                sendAlertMail(['qian.hong@dragonins.com','manman.zhang@dragonins.com','zhanghy@unair.cn','jianbo.li@unair.cn'],'卡航投递华泰出错，赔款限额不存在','运单'+str(row[10])+'的赔款限额'+str(row[17])+'不存在，烦请及时查看处理')
                continue
                # raise Exception("赔款限额不存在")

            insuranceObject['plan'] = plan # 款别
            insuranceObject['srcCPlyNo'] = '' # 不必填
            insuranceObject['prmCur'] = '01' 
            insuranceObject['premium'] = str(insuranceFee) # 保险费
            insuranceObject['amtCur'] = '01'
            insuranceObject['amount'] = row[18] #
            insuranceObject['rate'] = rate # policyRate 去除百分号后乘以10 [:-1] 截取从头开始到倒数第一个字符之前
            insuranceObject['effectiveTime'] = str(datetime.datetime.strptime(row[36],'%Y/%m/%d %H:%M:%S')) # 保险起期 departDateTime
            insuranceObject['terminalTime'] = str(datetime.datetime.strptime(insuranceObject['effectiveTime'],'%Y-%m-%d %H:%M:%S')+ datetime.timedelta(days = 15)) # 上面时间+15天
            insuranceObject['copy'] = '1' # 份数 
            insuranceObject['docType'] = '' # 不必填
            insuranceObject['docSN'] = '' # 不必填

            # 需要提供开票的六项信息
            appntObject = {}
            appntObject["appName"] = "上海淮链供应链管理有限公司" # 投保人姓名 custCName
            appntObject["appType"] = '2' 
            # appntObject["appBirthday"] = '' # 不必填
            appntObject["appEmail"] = '' # 不必填
            appntObject["appGender"] = '' # 不必填
            appntObject["appIDType"] = '97' 
            appntObject["appNumber"] = '91310230MA1JW5R81J'# 被保人证件号
            appntObject["appTelNumber"] = "021-20257919" # 投保人电话号
            appntObject["appAddr"] = "上海市浦东新区峨山路505号6楼"  # 地址信息
            appntObject["appContact"] = "" # 联系人名字
            appntObject["isTaxInvoice"] = '1' 
            appntObject["taxCertifi"] = "91310230MA1JW5R81J" # 税务登记证号
            appntObject["depositBank"] = "招商银行上海分行中山支行" # 开户银行
            appntObject["bankAccount"] = "121936848110902" # 银行账户

            insuredObject = {}
            insuredObject["insuredName"] = row[3] # 被保险人名称
            insuredObject["insuredType"] = '2' # 
            insuredObject["insuredEmail"] = '' # 不必填
            insuredObject["InsuredGender"] = '' # 不必填
            insuredObject["insuredIDType"] = '06' # 被保人证件类型

            insuredObject["insuredNumber"] = str(insuredData[0][7]) # 被保人证件号

            insuredObject["insuredTelNumber"] = '不详' # 被保险人电话
            insuredObject["insuredAddress"] = '' # 不必填
            insuredObject["relationship"] = '' # 不必填
            insuredObject["isLegal"] = '1' # 
            definedSafeObj = {}
            definedSafeObj["isDefinedSafe"] = '1' # 默认0，此节点不用传
            rdrDTOs=[]
            rdrs = {}
            rdrs["rdrCode"]='01'
            rdrs["rdrName"]='综合险'
            rdrs["rdrAmount"]= rdrAmount
            rdrs["rdrPremium"]= str(insuranceFee)
            rdrs["rdrDeductible"]= policyDeductible
            rdrDTOs.append(rdrs)
            definedSafeObj["rdrDTOs"] = rdrDTOs

            payObject = {} # 支付信息
            payObject["isSinglePay"] = '0' # 是否逐单支付
            
            agreementObject = {}
            agreementObject["policyDeductible"] = policyDeductible # 免赔额/率
            agreementObject["policySpec"] =  "" # 特别约定

            productDiffObject = {}

            productDiffObject["reMark"] = '' # 默认为空
            productDiffObject["vehicleNum"] = row[14] # 运单号 shipid
            productDiffObject["vehicleModel"] = '*'
            productDiffObject["vehicleLen"] = '*'
            productDiffObject["vehicleFrameNum"] = '*' 
            productDiffObject["goodsName"] = str(row[27])+'（载运输工具：'+str(row[15])+'）' # 货物名称 cargoName

            productDiffObject["goodsQuantity"] = row[37] # 货物数量 cargoCount
            productDiffObject["goodsPack"] = '08' # 包装方式
            productDiffObject["goodsValue"] = row[18] # 货物价值 cargeFreight
            departSpot=str(row[45])
            if departSpot=='Default':
                departSpot=''
            productDiffObject["transFrom"] = row[28]+row[29]+row[30]+departSpot #  省、市、区（departProvince + departCity + departDistrict+departSpot）
            transDepot=str(row[46])
            if transDepot=="Default":
                transDepot=''
            productDiffObject["transDepot"] = transDepot # 中转地
            deliveryAddress=str(row[42])
            if deliveryAddress=='Default':
                deliveryAddress=''
            productDiffObject["transTo"] = row[31]+row[32]+row[33]+deliveryAddress # 目的地 省、市、区（destinationProvice + destinationCity + destinationDistrict+deliveryAddress
            productDiffObject["transDate"] = str(datetime.datetime.strptime(row[36],'%Y/%m/%d %H:%M:%S')) # 起运日期
            productDiffObject["transportCost"] = row[16] # 运费


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
            print(Json)          
            m = hashlib.md5()
            b = (str(Json2) + key).encode(encoding='utf-8')
            m.update(b)
            signmd5 = m.hexdigest()
            # print(signmd5)

            #写入日志
            log_file = open('logs/' + datetime.datetime.now().strftime("%Y%m%d%H%M%S%f") +'_KHtoHT.log',mode='a', encoding='utf-8')
            log_file.write('---------------------------发给华泰报文 ' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '---------------------------\n')
            log_file.write(str(Json)+'\n')
            #log_file.write(signmd5)
            
            #post出单接口请求
            # 通过字典方式定义请求body
            FormData = {"json": str(Json), "channelCode": str(channelObject["channelCode"]), "signature": str(signmd5)}
            data = parse.urlencode(FormData)
            headers = {
                'Content-Type': "application/x-www-form-urlencoded",
            }
            # 请求方式
            content = requests.post(url=url, headers=headers, data=data).text
            print(content)
            content = json.loads(content)
            log_file.write('---------------------------对接华泰结果 ' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '---------------------------\n')
            log_file.write(str(content))
            log_file.close()
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
                sendAlertMail(['manman.zhang@dragonins.com','qian.hong@dragonins.com'],'卡航-对接华泰',str(guiderr) + '<br />' + str(error))
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
                sendAlertMail(['manman.zhang@dragonins.com','qian.hong@dragonins.com'],'卡航-对接华泰',str(guiderr) + '<br />' + str(error)) 
            # # 回写remotedata投保表
            sql = "UPDATE remotedata SET Status = '"+_Status+"', errLog = '"+_responseInfo+"', policySolutionID = '"+_policyNO+"', relationType = '"+_policyURL+"'  WHERE guid = '"+guid+"'"
            cursor.execute(sql) #执行sql 语句
            conn.commit() #提交

            #反馈卡航
            custProperty=str(row[49])
            biz=str(row[11])
            result2 = ""
            if _responseCode!="2":
                if biz != "冲抵余额":
                    if custProperty== "BS":
                        result2=postToQL_BS_Old(guid)
                    else:
                        result2=postToQL_BS(guid)
    except Exception as err:
        traceback.print_exc()
        print("请求失败",err) 
        sendAlertMail(['qian.hong@dragonins.com','manman.zhang@dragonins.com'],'卡航投递华泰出错',str(err)+'<br />' + str(FormData))

def postToQL_BS_Old(guid):
    try:
        conn = pymssql.connect(host="121.36.193.132",port = "15343",user="sa",password="sate1llite",database="kahang",charset='utf8')
        cursor = conn.cursor() #创建一个游标对象，python 里的sql 语句都要通过cursor 来执行
        sql = "select * from RemoteData where guid='"+guid+"'" 
        cursor.execute(sql)   #执行sql语句
        data = cursor.fetchall()  #读取查询结果
        
        channelOrderId = str(data[0][10])
        certificateNo = ""
        validFrom = ""
        isSuccess = ""
        policyDownloadUrl = ""
        retMsg = ""
        if str(data[0][52])=='投保成功':
            certificateNo = str(data[0][69])
            validFrom = str(data[0][36])
            policyDownloadUrl = str(data[0][44])
            isSuccess = "成功"
            retMsg = ""
        else:
            isSuccess = "失败"
            retMsg=str(data[0][53])

        url="http://150.242.238.194:66/api/LongKunReturnBL"
        jsons = "{channelOrderId:\"" + channelOrderId + "\",certificateNo:\"" + certificateNo + "\",validFrom:\"" + validFrom+ "\",isSuccess:\"" + isSuccess + "\",policyDownloadUrl:\"" + policyDownloadUrl + "\",retMsg:\"" + retMsg + "\"}"
        datajson = jsons.encode('UTF-8')
        headers = {'Content-Type': "application/x-www-form-urlencoded"}
        jsonResult = requests.post(url=url, headers=headers, data=datajson).text
        errlog=str(data[0][53])+ " 客户反馈接口：" +jsonResult.replace('\'','"')
        
        sql="update remotedata set errLog='"+str(errlog)+"' where guid='"+str(guid)+"'"
        cursor.execute(sql)
        conn.commit()

        #反馈给乾龙第二个地址
        url="http://150.242.238.194:67/api/LongKunPolicy?"
        headers = {'Content-Type': "application/x-www-form-urlencoded"}
        jsonResult = requests.post(url=url, headers=headers, data=datajson).text
        return "反馈成功"
    except Exception as err:
        traceback.print_exc()
        sendAlertMail(['qian.hong@dragonins.com','manman.zhang@dragonins.com'],'反馈卡航旧通道出错,guid='+str(guid),str(err)+'<br />')
        return "反馈失败"

def postToQL_BS(guid):
    try:
        conn = pymssql.connect(host="121.36.193.132",port = "15343",user="sa",password="sate1llite",database="kahang",charset='utf8')
        cursor = conn.cursor() #创建一个游标对象，python 里的sql 语句都要通过cursor 来执行
        sql = "select * from RemoteData where guid='"+guid+"'" 
        cursor.execute(sql)   #执行sql语句
        data = cursor.fetchall()  #读取查询结果
    
        channelOrderId = str(data[0][10])
        certificateNo = ""
        validFrom = ""
        isSuccess = ""
        policyDownloadUrl = ""
        retMsg = ""
        if str(data[0][52])=='投保成功':
            certificateNo = str(data[0][69])
            validFrom = str(data[0][36])
            policyDownloadUrl = str(data[0][44])
            isSuccess = "成功"
            retMsg = ""
        else:
            isSuccess = "失败"
            retMsg=str(data[0][53])

        MD5str = hashlib.md5("channelOrderId:\"" + channelOrderId + "\"") +hashlib.md5("retMsg:\"" + retMsg + "\"") +hashlib.md5("certificateNo:\"" + certificateNo + "\"") +hashlib.md5("isSuccess:\"" + isSuccess + "\"") +hashlib.md5("policyDownloadUrl:\"" + policyDownloadUrl + "\"") +hashlib.md5("validFrom:\"" + validFrom + "\"")
        MD5str = hashlib.md5("api_" + MD5str + "_api")
        jsons = "channelOrderId=" + channelOrderId + "&retMsg=" + retMsg + "&certificateNo=" + certificateNo + "&isSuccess=" + isSuccess + "&" +"policyDownloadUrl=" + policyDownloadUrl + "&validFrom=" + validFrom + "&token=20e35feae64fcb2159485ec821606799"
        url="http://khtms.unair.cn/index.php/api/Longkunreturn/receptionLongkun"
        datajson = jsons.encode('UTF-8')
        headers = {'Content-Type': "application/x-www-form-urlencoded"}
        jsonResult = requests.post(url=url, headers=headers, data=datajson).text

        errlog=str(data[0][53])+ " 客户反馈接口：" + jsonResult.replace('\'','"')
        sql="update remotedata set errLog='"+str(errlog)+"' where guid='"+str(guid)+"'"
        cursor.execute(sql)
        conn.commit()
        return "反馈成功"
    except Exception as err:
        traceback.print_exc()
        sendAlertMail(['qian.hong@dragonins.com','manman.zhang@dragonins.com'],'反馈卡航新通道出错,guid='+str(guid),str(err)+'<br />')
        return "反馈失败"

issueInterface() # 调用华泰出单接口

         
