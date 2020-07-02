import json
import pymssql
import requests
import traceback
import datetime
from dateutil.relativedelta import relativedelta
import decimal

try:
    # 打开数据库连接
    conn = pymssql.connect(host="121.36.193.132",port = "15343",user="sa",password="sate1llite",database="insurance",charset='utf8')
    cursor = conn.cursor() #创建一个游标对象，python 里的sql 语句都要通过cursor 来执行
    sql = "select top (1)* from RemoteData left join ValidInsured on RemoteData.appkey = ValidInsured.Appkey where RemoteData.appkey='4a33b1fe29333104b90859253f4d1b68' order by CreateDate desc  "   
    cursor.execute(sql)   #执行sql语句
    data = cursor.fetchall()  #读取查询结果
    cursor.close()
    conn.close()
    # 打？不懂
    for row in data: 
        result={}

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
        insuranceObject['prmCur'] = '01' # ?
        insuranceObject['premium'] = row[21] # 保险费
        insuranceObject['amtCur'] = '01'
        insuranceObject['amount'] = '12000.0' # ?
        insuranceObject['rate'] = str(decimal.Decimal(row[68][:-1]) * 10) # policyRate 去除百分号后乘以10 [:-1] 截取从头开始到倒数第一个字符之前
        insuranceObject['effectiveTime'] = '2016-01-27 00:00:00' # ?
        insuranceObject['terminalTime'] = '2020-06-30 17:27:28' # ?
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
        insuredObject1["insuredType"] = '2' # ?
        insuredObject1["insuredBirthday"] = '1989-10-02' # ?
        insuredObject1["insuredEmail"] = '111@qq.com' # ?
        insuredObject1["InsuredGender"] = '0' # ?
        insuredObject1["insuredIDType"] = '01' # ?
        insuredObject1["insuredNumber"] = '340881198910020112' # ?
        insuredObject1["insuredTelNumber"] = '18721662166' # ?
        insuredObject1["insuredAddress"] = '被保险人地址' # ?
        insuredObject1["relationship"] = '5' # ?
        insuredObject1["isLegal"] = '1' # ?
        insuredObject1["benefitDTOs"] = []
        # end---------------------------------------------

        # --------- 以下整段问华泰
        definedSafeObj = {}
        definedSafeObj["isDefinedSafe"] = '0' # 先写0，以下暂时不用传
        # start----暂时不传--------
        rdrDTOs = {}
        rdrDTOs["rdrCode"] = "01" # ?
        rdrDTOs["rdrName"] = "意外伤害"# ?
        rdrDTOs["rdrAmount"] = "6000"# ?
        rdrDTOs["rdrPremium"] = "100"# ?
        rdrDTOs["rdrDeductible"] = "5000元"# ?
        rdrDTOs["rdrRemark"] = "意外医疗费用"# ?

        rdrDTOs1 = {}
        rdrDTOs1["rdrCode"] = "02" # ?
        rdrDTOs1["rdrName"] = "意外伤害"# ?
        rdrDTOs1["rdrAmount"] = "6000"# ?
        rdrDTOs1["rdrPremium"] = "100"# ?
        rdrDTOs1["rdrDeductible"] = "5000元"# ?
        rdrDTOs1["rdrRemark"] = "意外医疗费用"# ?

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


        result["channelObject"] = channelObject
        result["insuranceObject"] = insuranceObject
        result["appntObject"] = appntObject
        # result["insuredObject"] = [insuredObject,insuredObject1]
        result["insuredObject"] = [insuredObject]
        result["definedSafeObj"] = definedSafeObj
        result["agreementObject"] = agreementObject
        result["productDiffObject"] = productDiffObject
        Json = "data=xxx||"+ json.dumps(result)
        print(Json)

except Exception as err:
    traceback.print_exc()
    print("请求失败",err) 