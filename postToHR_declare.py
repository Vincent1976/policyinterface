import json
import pymssql
import requests
import traceback
import datetime
from dateutil.relativedelta import relativedelta
import decimal
import config
from dals import dal



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
        channelObject["bizCode"]= '121' # ?
        channelObject["channelCode"]='100000' # ?
        channelObject["channelName"]='XXXXX' # ?
        channelObject["orderId"]= row[14] # 订单号 shipid
        channelObject["createTime"]= str(datetime.datetime.now()) # 当前时间

        insuranceObject = {}
        insuranceObject["insuranceCode"] = '4604' # ?
        insuranceObject["insuranceName"] = '旅行险' # ?
        insuranceObject['plan'] = 'A' # ?
        insuranceObject['srcCPlyNo'] = '' # 不必填
        insuranceObject['prmCur'] = '01' 
        insuranceObject['premium'] = row[21] # 保险费
        insuranceObject['amtCur'] = '01'
        insuranceObject['amount'] = '12000.0' 
        insuranceObject['rate'] = str(decimal.Decimal(row[68][:-1]) * 10) # policyRate 去除百分号后乘以10 [:-1] 截取从头开始到倒数第一个字符之前
        insuranceObject['effectiveTime'] = '2016-01-27 00:00:00' # 投保时间or起运日期，早者为准
        insuranceObject['terminalTime'] = '2020-06-30 17:27:28' # 上面时间+30日
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
        insuredObject["insuredIDType"] = '99' 
        insuredObject["insuredNumber"] = '不详' 
        insuredObject["insuredTelNumber"] = '不详' 
        insuredObject["insuredAddress"] = '' # 不必填
        insuredObject["relationship"] = '' # 不必填
        insuredObject["isLegal"] = '1' # 

        # benefitDTOs 当 isLegal = 1时 不传
        # start-----------不传------------------
        benefitDTOs= {}
        benefitDTOs["benefitName"] = 'xx1' # ?
        benefitDTOs["benefitRelation"] = '10' # ?
        benefitDTOs["benefitListNumber"] = '1' # ?
        benefitDTOs["share"] = '50' # ?
        benefitDTOs["benefitIDType"] = '01' # ?
        benefitDTOs["benefitNumber"] = '429006199203045612' # ?

        benefitDTOs1= {}
        benefitDTOs1["benefitName"] = 'xx2' # ?
        benefitDTOs1["benefitRelation"] = '10' # ?
        benefitDTOs1["benefitListNumber"] = '2' # ?
        benefitDTOs1["share"] = '50' # ?
        benefitDTOs1["benefitIDType"] = '01' # ?
        benefitDTOs1["benefitNumber"] = '429006199203045613' # ?
        insuredObject["benefitDTOs"] = [benefitDTOs1,benefitDTOs1]
        # end---------------------------------------------


        # start-------------------不传--------------------------
        insuredObject1 = {}
        insuredObject1["insuredName"] = row[9] # 被保险人名称
        insuredObject1["insuredType"] = '' 
        insuredObject1["insuredBirthday"] = '' 
        insuredObject1["insuredEmail"] = ''
        insuredObject1["InsuredGender"] = '' 
        insuredObject1["insuredIDType"] = '' 
        insuredObject1["insuredNumber"] = '' 
        insuredObject1["insuredTelNumber"] = '' 
        insuredObject1["insuredAddress"] = '' 
        insuredObject1["relationship"] = '' 
        insuredObject1["isLegal"] = '' 
        insuredObject1["benefitDTOs"] = []
        # end---------------------------------------------

        # --------- 以下整段问华泰
        definedSafeObj = {}
        definedSafeObj["isDefinedSafe"] = '0' # 先写0，以下暂时不用传
        # start----暂时不传--------
        rdrDTOs = {}
        rdrDTOs["rdrCode"] = "" 
        rdrDTOs["rdrName"] = ""
        rdrDTOs["rdrAmount"] = ""
        rdrDTOs["rdrPremium"] = ""
        rdrDTOs["rdrDeductible"] = ""
        rdrDTOs["rdrRemark"] = ""

        rdrDTOs1 = {}
        rdrDTOs1["rdrCode"] = "" 
        rdrDTOs1["rdrName"] = ""
        rdrDTOs1["rdrAmount"] = ""
        rdrDTOs1["rdrPremium"] = ""
        rdrDTOs1["rdrDeductible"] = ""
        rdrDTOs1["rdrRemark"] = ""

        definedSafeObj["rdrDTOs"] = [rdrDTOs,rdrDTOs1]
        
        agreementObject = {}
        agreementObject["policyDeductible"] = '6000元' # ?
        agreementObject["policySpec"] = '特别约定' # ?
        # end---------------
        # -----------------------

        productDiffObject = {}

        productDiffObject["reMark"] = '' # 默认为空
        productDiffObject["vehicleNum"] = '车牌号' # ?
        productDiffObject["vehicleModel"] = '车型' # ?
        productDiffObject["vehicleLen"] = '车长' # ?
        productDiffObject["vehicleFrameNum"] = '车架号' # ?
        productDiffObject["goodsName"] = row[27] # 货物名称 cargoName

        productDiffObject["goodsQuantity"] = row[37] # 货物数量 cargoCount
        productDiffObject["goodsPack"] = '包装方式' # ? 问华泰（客户传递的是手填的文字，无法匹配）
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

        #写入日志
        # log_file = open('logs/' + datetime.datetime.now().strftime("%Y%m%d%H%M%S%f") +'_huatai.log',mode='a')
        # log_file.write('---------------------------发给华泰报文 ' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '---------------------------\n')
        # log_file.write('---------------------------对接华泰结果 ' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '---------------------------\n')
        # log_file.write(str(Json))
        # log_file.close()
        
        #post接口请求
        url="http://219.141.242.74:9039/service_platform/GeneralUnderInterface"
        result = Json
        headers = {
            'Content-Type': "application/json",
        }
        response = requests.request("POST", url, data=result, headers=headers)

        _bizCode = ""
        _channelCode = ""
        _orderId = ""
        _responseInfo = ""
        _proposalNo = ""
        _Status = ""
        _responseCode = response.json()['responseCode'] # 接收返回的参数
        if _responseCode == "2": # 人工核保
            _bizCode = response.json()['bizCode'] 
            _channelCode = response.json()['channelCode'] 
            _orderId = response.json()['orderId'] 
            _responseInfo = response.json()['responseInfo'] 
            _proposalNo = response.json()['proposalNo']
            _Status = "人工核保" 
        elif _responseCode == "1": # 核保通过
            _bizCode = response.json()['bizCode'] 
            _channelCode = response.json()['channelCode'] 
            _orderId = response.json()['orderId'] 
            _responseInfo = response.json()['responseInfo'] 
            _proposalNo = response.json()['proposalNo']
            _Status = "投保成功" 
        else: # 投保失败
            _bizCode = response.json()['bizCode'] 
            _channelCode = response.json()['channelCode'] 
            _orderId = response.json()['orderId'] 
            _responseInfo = response.json()['responseInfo'] 
            _proposalNo = response.json()['proposalNo']
            _Status = "投保失败" 
        # 回写remotedata投保表
        sql = "UPDATE remotedata SET Status='%s', errLog='%s',shipid = '%s' WHERE guid='%s'" %(_Status, _responseInfo, _orderId, '93d50a8c-bd09-11ea-a718-fa163eba4024')
        dal.SQLHelper.update(sql,None)

except Exception as err:
    traceback.print_exc()
    print("请求失败",err) 