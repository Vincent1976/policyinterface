import json
import pymssql
import requests
import traceback
import datetime
from dateutil.relativedelta import relativedelta
import decimal
from dals import dal
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
        sql = "select top (1) *,datediff(second,CreateDate,departDateTime) 'diff' from RemoteData where RemoteData.appkey='03D9AC28-3AF5-488C-9F5A-2EDD41331F8A' and RemoteData.status = '等待投保' order by CreateDate desc" 

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
            channelObject["bizCode"]= '121' # 交易类型
            channelObject["channelCode"]='100189' # 渠道编码
            channelObject["channelName"]='上海励琨互联网科技有限公司' # 渠道名称
            channelObject["orderId"]= row[14] # 订单号 shipid
            channelObject["createTime"]= str(datetime.datetime.now())[0:19] # 当前时间

            # 校验最低保费
            claimLimit= float(row[17])
            cargoFreight=float(row[16])
            insuranceFee=float(row[21])
            rate=str(decimal.Decimal(row[68][:-1]) * 10)
            policyDeductible=''
            if claimLimit==300000:
                policyDeductible='RMB1,000元或损失金额的5%,以高者为准'
                rate='1.5'
                insuranceFee=cargoFreight*0.0015
                if insuranceFee<=3.5:
                    insuranceFee=3.5
            elif claimLimit==2000000:
                policyDeductible='RMB2,000元或损失金额的5%,以高者为准'
                rate='2'
                insuranceFee=cargoFreight*0.002
                if insuranceFee<=10:
                    insuranceFee=10
            elif claimLimit==5000000:
                policyDeductible='RMB3,000元或损失金额的5%,以高者为准'
                rate='2.5'
                insuranceFee=cargoFreight*0.0025
                if insuranceFee<=25:
                    insuranceFee=25
            else:
                raise Exception("赔款限额不存在")

            insuranceObject = {}
            insuranceObject["insuranceCode"] = '362205' # 险种代码
            insuranceObject["insuranceName"] = '承运人公路货运责任保险条款 ' # 产品名称
            insuranceObject['plan'] = 'A' # 款别
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
            definedSafeObj["isDefinedSafe"] = '0' # 默认0，此节点不用传

            payObject = {} # 支付信息
            payObject["isSinglePay"] = '0' # 是否逐单支付
            
            agreementObject = {}
            agreementObject["policyDeductible"] = policyDeductible # 免赔额/率
            agreementObject["policySpec"] =  "1. 如被保险人的运输业务涉及航空运输，或陆运与空运的联运业务，本保险扩展承保被保险人以空运方式运输货物因条款及本协议列明的承保风险所产生的货损赔偿责任。2.针对航空运输及陆运与空运的联运业务，若单次运输赔偿限额超过人民币200万元的，需提前一个工作日向保险人申报，经保险人与被保险人双方确认承保条件后方可承保。但以下列明的被保险人单次运输赔偿限额最高可为人民币500万元：1）成都锦程好来运物流有限公司2）成都世纪天航物流有限公司3）广州熔鑫国际物流有限公司4）武汉星晨货运服务有限公司3.本保险扩展承保被保险人因承运货物被盗窃、抢劫所致的货损赔偿责任的风险。盗窃、抢劫案件的保单责任认定，必须以警方出具的立案证明及未破案证明为前提（5万以上（含5万）的空运货物盗窃、抢劫、神秘失踪必须出示机场公安局的事故立案证明，2万至5万（含2万）的空运货物盗窃、抢劫、神秘失踪可以提供机场派出所的报案回执单以及航空公司出具的货损/货差证明，2万以下的空运货物盗窃、抢劫、神秘失踪可以航空公司出具的货损/货差证明）。4. 任何情况下，整车神秘失踪（司机、车辆、货物同时失踪）、整车遭遇盗抢（车辆、货物同时被盗窃或抢劫，或因承运车辆被盗造成的货物损失）、货物遭遇哄抢、货物或承运车辆遭受恶意破坏、实际承运人道德风险、无人看管情况下的存车过夜过程中的损失均不属于保单覆盖范围。5. 保单覆盖地域范围：中华人民共和国境内（港、澳、台地区除外），在保单覆盖地域范围以外地区发生的事故及造成的损失均为本保单除外责任。6. 承运车辆必须符合道路/桥梁/隧道/立交桥/限宽门（桩）等等各类道路设施的通行规定或要求，对于因此造成的任何损失，本保单的赔偿责任仅限于扣除该次事故免赔后剩余损失金额的50％，且无论如何，该次事故的赔偿限额不超过人民币50 万元；7. 在被保险人不是实际承运人的情况下，保险人保留向实际承运人追偿的权利。8. 废旧品、二手品、展览后之展品和陈列品仅承保火灾爆炸、交通工具发生意外事故造成的损失。废旧品、二手品和裸装货物不负责锈损和刮擦导致的损失。9. 易碎品，专业承运人承运时必须使用专业化包装进行运输，仅承保因火灾、爆炸、交通事故造成保险标的的损失。10.被保险人不是实际承运人的情况下，被保险人必须将有关业务委托给有资质的并具备合格驾驶证、行驶证、营运证的专业承运人进行运输，否则保险人不负责赔偿造成的任何损失、费用和责任。11.针对手机、笔记本电脑、数码相机等便携式设备的整车运输，必须采用封闭式箱式货车运输，发生盗窃、抢劫事故时，每个/台最高赔偿限额RMB5000元。针对上述货物采取零担运输或者航空运输以及含有该等运输的多式联运的，发生盗窃（包括提货不着）、抢劫，除适用本条最高赔偿限额的约定外，每次事故免赔为5000元或损失金额的20%，以高者为准。12.药品纳入本保单承保范围（疫苗除外），此类货物发生损失时，本保单只负责赔偿受损货物的直接损失（如外包装破损，但内包装完好，本保单仅就受损的外包装承担赔偿责任），不承担由此产生的整箱货物被拒收或无法销售带来的损失以及其他间接损失。13 保险人赔偿被保险货物全部或部分损失后，残值的处置权利归于保险人。除非保单存在其他特别约定，否则被保险人不得私自出售或以其他方式处置受损被保险货物。经保险人同意由被保险人处置残值的，保险人保留对残值的定价权利，保险人有权扣除该部分金额后对受损被保险货物予以赔付。14.本保单仅覆盖运输过程中的临时仓储，临时仓储以48小时为限（货物起运前存放于货主仓储地的和运抵目的地货物签收交接完成后的仓储为本保单除外责任）。48小时临时仓储的每次赔偿限额：人民币40万元，年累计赔偿限额：人民币80万元。15.本协议项下保单为系统自动传输相关承保货物，保险公司无法通过系统校验货物名称是否属于不保货物，无论投保人在数据传输时是否包括不保货物，在被保险人申请保险理赔时，如果该货物属于除外货物，保险公司均有权不承担保险责任。如若投保人未能及时传输数据，保险公司有权不负责赔偿责任。16.赔偿金额以实际申报的每次事故赔偿限额和实际货物损失的低者为准。17.如被保险人有变动，需提前7个工作日向保险人提出书面批改申请，保险人同意后方可生效。18.本保单扩展承保冷藏品（疫苗除外）。下列原因导致的与冷藏品（疫苗除外）有关的任何损失为除外责任：1) 由于冷藏机器或隔温设备的损坏或者车厢内贮存冰决的溶化所造成的解冻溶化而腐败的损失；2) 货物本身属性造成的损失；3) 延迟交货造成的损失；4) 货价下跌造成的损失；5) 气温，湿度，串味造成的损失；6) 无外来事故情况下鲜活货品的变质，死亡造成的损失。19.本保单扩展陆路运输的木材（原木除外）" # 特别约定

            productDiffObject = {}

            productDiffObject["reMark"] = '' # 默认为空
            productDiffObject["vehicleNum"] = row[14] # 运单号 shipid
            productDiffObject["vehicleModel"] = '*'
            productDiffObject["vehicleLen"] = '*'
            productDiffObject["vehicleFrameNum"] = '*' 
            productDiffObject["goodsName"] = row[27] # 货物名称 cargoName

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
            Json2 = Json.replace("%", "%25").replace("&", "%26").replace("\\+", "%2B")
            print(Json)          
            key = "123456@HT" # 线下提供的密钥
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
            url="http://219.141.242.74:9039/service_platform/InsureInterface"
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
                sendAlertMail('manman.zhang@dragonins.com','卡航-对接华泰',str(guiderr) + '<br />' + str(error))
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
                sendAlertMail('manman.zhang@dragonins.com','卡航-对接华泰',str(guiderr) + '<br />' + str(error)) 
            # # 回写remotedata投保表
            sql = "UPDATE remotedata SET Status = '"+_Status+"', errLog = '"+_responseInfo+"', policySolutionID = '"+_policyNO+"', relationType = '"+_policyURL+"'  WHERE guid = '"+guid+"'"
            cursor.execute(sql) #执行sql 语句
            conn.commit() #提交
    except Exception as err:
        traceback.print_exc()
        print("请求失败",err) 
        sendAlertMail(['qian.hong@dragonins.com','manman.zhang@dragonins.com'],'卡航投递华泰出错',str(err)+'<br />' + str(FormData))

issueInterface() # 调用华泰出单接口

         
