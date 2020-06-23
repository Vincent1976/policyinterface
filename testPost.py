import json
import pymssql #引入pymssql模块
import requests
import traceback

try:
    # 打开数据库连接
    conn = pymssql.connect(host="121.36.204.186",port = "15343",user="sa",password="sate1llite",database="newInsurance",charset='utf8')

    # if conn:
    #     print("连接成功!")
    cursor = conn.cursor() #创建一个游标对象，python 里的sql 语句都要通过cursor 来执行
    sql = "select top 1 * from RemoteData order by CreateDate desc"   
    # sql = "select  * from RemoteData where guid = 'ab5e60e0-f245-4ddb-9187-50bcfd23ebc9'"

    cursor.execute(sql)   #执行sql语句
    data = cursor.fetchall()  #读取查询结果
    cursor.close()
    conn.close()
    for row in data: 

        result={} 
        result["PartyFunctionCode"]='BM'
        result["LogisticsExchangeCode"]='d9f81ac8-0ede-45bb-843b-f69dc648237d'
        result["PartyName"]=row[3]
        result["CodeListQualifier"] = "175"
        result["UnifiedSocialCreditIdentifier"] = ""
        result["UnitName"] = row[4]
        result["UnitAddress"] = row[45]
        result["UnitPhone"] = row[6]
        result["UnitEmail"] = row[5]

        result1={} 

        result1["PartyFunctionCode"]='BN'
        result1["LogisticsExchangeCode"]='d9f81ac8-0ede-45bb-843b-f69dc648237d'
        result1["PartyName"]=row[9]
        result1["CodeListQualifier"] = "175"
        result1["UnifiedSocialCreditIdentifier"] = ""
        result1["UnitName"] = 'null'
        result1["UnitAddress"] = row[42]
        result1["UnitPhone"] = "13221289472"
        result1["UnitEmail"] = ""

        result2 = {}
        result2["InsureDateTime"]= row[36][0:4]+row[36][5:7]+row[36][8:10]+row[36][11:13]+row[36][14:16]+row[36][17:19]
       
        result2["InsuranceCode"] = "YII"
        # result2["InsuranceCoverageName"] = row[66]
        result2["InsuranceCoverageName"] = "太平洋国内水路、陆路货物运输保险条款"
        result2["InsuranceCoverageCode"] = "LK015004"
        result2["InsuranceCoverageTypeCode"] = "null"
        result2["InsurableValue"] = ""
        result2["SumInsuredMonetaryAmount"] = row[18]
        result2["ChargeTypeCode"] = row[67]
        result2["Rate"] = row[68]
        result2["deductible"] = row[12]
        result2["MonetaryAmount"] = row[21]
        result2["PriceCurrencyCode"] = "CNY"

        result3 = {}
        result3["CodeListQualifier"] = "175"
        result3["PartyName"] = "深圳德坤物流有限公司"
        result3["TaxpayerIdentifyNumber"] = "91440300087793129B"
        result3["ComAddress"] = "深圳市宝安区福永街道怀德社区怀德西部物流园B17-26档"
        result3["ComTel"] = "0755-22315619"
        result3["ComBankName"] = "华夏银行深圳龙岗支行"
        result3["ComBankNo"] = "10864000000524885"


        result5={}
        result5["PlaceLocationQualifier"] = 5
        result5["PlaceOrLocation"] = row[46]
        result5["CountrySubdivisionCode"] = ""
        result5["PostalIdentificationCode"] = "null"
        result5["PlaceProvince"] = row[28]
        result5["PlaceCity"] = row[33]
        result5["PlaceDistrict"] = ""


        result6={}
        result6["PlaceLocationQualifier"] = 8
        result6["PlaceOrLocation"] = row[42]
        result6["CountrySubdivisionCode"] = ""
        result6["PostalIdentificationCode"] = "null"
        result6["PlaceProvince"] = row[31]
        result6["PlaceCity"] = row[32]
        result6["PlaceDistrict"] = row[33]

        result7={}
        result7["ShippingNoteNumber"] = ""
        result7["DescriptionOfGoods"] = row[27]
        result7["CargoTypeClassificationCode"] = ""
        result7["CargoTypeClassification1"] = row[26]
        result7["CargoTypeClassification2"] = ""
        result7["PackageType"] = row[25]
        result7["PackageTypeCode"] = ""
        result7["PackageQuantity"] = row[37]
        result7["PackageUnit"] = row[62]

        result8={}
        result8["GoodsItemGrossWeight"] = "100.00"
        result8["MeasurementUnitCode"] = "kg"

        result9={}
        result9["Cube"] = "0.30"
        result9["MeasurementUnitCode"] = "m3"

        result4 = {}
        # result4["OriginalDocumentNumber"] = row[46]
        result4["OriginalDocumentNumber"] = "333"

        result4["VehicleClassification"] = ""
        result4["VehicleClassificationCode"] = ""
        result4["VehicleNumber"] = row[15]
        result4["VehicleAge"] = ""
        result4["TrailerVehiclePlateNumber"] = ""
        result4["TrailerVehicleAge"] = ""
        result4["ModeOfTransport"] = ""
        result4["TransportModeCode"] = row[22]
        result4["ShippingType"] = row[23]
        result4["LotType"] = row[24]
        result4["Remark"] = ""
        result4["PlaceOrLocationInformation"] = [result5,result6]
        result4["GoodsInformation"] = [result7]
        result7["ItemGrossWeight"] = result8
        result7["ItemVolume"] = result9

        list=[result,result1]
        list1=[result2]
        list2=[result3]
        list3=[result4]


        PartyInformation={}
        TransportInformation = {}

        PlaceOrLocationInformation={}
        PartyInformation["SequenceCode"]=row[10]
        # PartyInformation["SequenceCode"]="777777"  
        PartyInformation["shipId"]=row[14]
        PartyInformation["departCity"]=row[29]
        PartyInformation["destinationDistrict"]=row[33]
        PartyInformation["cargocount"]=row[37] 
        PartyInformation["cargoWeight"]=row[38] 

 
 
        PartyInformation["InsuranceBillCode"]=row[41]
        PartyInformation["PartyInformation"]=list
        PartyInformation["ChargeInformation"]=list1
        PartyInformation["InvoiceInformation"] = list2
        PartyInformation["TransportInformation"] = list3

        Json = "data=xxx||"+ json.dumps(PartyInformation)
        print (Json)

    #post接口请求
        url="http://127.0.0.1:5000/dekun?remoteuser=8CB22A57-50E6-4B4C-9F65-BA45B5D56F9D"
        data = Json
        headers = {
            'Content-Type': "application/json",
        }
        response = requests.request("POST", url, data=data, headers=headers)
        # print(response)
except Exception as err:
    traceback.print_exc()
    print("请求失败",err) 