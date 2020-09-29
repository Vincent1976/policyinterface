#coding=utf-8
import pymssql
import traceback
import datetime
import smtplib
from email.header import Header
from email.mime.text import MIMEText
import xlwt

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
        mail_pass = 'Sate1llite'   # 口令
        smtpObj = smtplib.SMTP()
        smtpObj.connect(mail_host, 25)    # 25 为 SMTP 端口号
        smtpObj.login(mail_user, mail_pass)
        smtpObj.sendmail(sender, receivers, message.as_string())
        print('邮件发送成功')
    except smtplib.SMTPException:
        print('Error: 无法发送邮件')

conn = pymssql.connect(host="121.36.204.186",port = "15343",user="sa",password="sate1llite",database="newInsurance",charset='utf8')
cursor = conn.cursor() #创建一个游标对象，python 里的sql 语句都要通过cursor 来执行
# 每日定时获取德坤前日所有投保数据发邮件给保险公司
def getDKRemoteDataToEmail():
    try:
        # 打开数据库连接
        sql = "select custCoName,insuredName, channelOrderId,licenseId,trafficType,packageType,cargoType,cargoName,departProvince,departCity,departDateTime,cargoCount,deliveryAddress,cargeValue,policyRate,insuranceFee from RemoteData where deliveryOrderId='深圳机场山东项目' and policyNo<>'TEST' and ExceptionStatus <>'已写入'" 
        cursor.execute(sql)   #执行sql语句
        data = cursor.fetchall()  #读取查询结果
        if len(data)!=0 :
            data1 =[]
            for row in data:
                pams = {}
                pams["custCoName"] = '深圳德坤物流有限公司及其下属分公司，子公司及控股公司'
                pams["insuredName"] = '实际货主'
                pams["channelOrderId"] = row[2]
                pams["licenseId"] = row[3]
                pams["trafficType"] = row[4]
                pams["packageType"] = row[5]
                pams["cargoType"] = row[6]
                pams["cargoName"] = row[7]
                pams["departProvince"] = row[8]
                pams["departCity"] = row[9]
                pams["departDateTime"] = row[10]
                pams["cargoCount"] = row[11]
                pams["deliveryAddress"] = row[12]
                pams["cargeValue"] = row[13]
                pams["policyRate"] = row[14]
                pams["insuranceFee"] = row[15]
                data1.append(pams)
            html = ""
            for i in range(len(data1)):
                html += "投保人："+data1[i].get("custCoName")+'<br/>'+"被保险人:" + data1[i].get("insuredName")+'<br/>'+"发车批次:" + data1[i].get("channelOrderId")+'<br/>' +"车牌号-车辆类型:" + data1[i].get("licenseId")+'<br/>'+"运输方式:" + data1[i].get("trafficType")+'<br/>' +"包装方式:" + data1[i].get("packageType")+'<br/>' +"货物类型:" + data1[i].get("cargoType")+'<br/>' "货物名称："+data1[i].get("cargoName")+'<br/>'+"起运省："+data1[i].get("departProvince")+'<br/>'+"起运市："+data1[i].get("departCity")+'<br/>'+"起运日期："+data1[i].get("departDateTime")+'<br/>'+"件数/重量："+data1[i].get("cargoCount")+'<br/>'+"目的地："+data1[i].get("deliveryAddress")+'<br/>'+"保额："+data1[i].get("cargeValue")+'<br/>'+"费率："+data1[i].get("policyRate")+'<br/>'+"保费："+data1[i].get("insuranceFee")+'<br/>'+'<br/>'+'<br/>'
            sendAlertMail(['zhangliyong-001@cpic.com.cn','dongping.yi@dragonins.com','shuxian.he@dragonins.com'],'【'+datetime.datetime.now().strftime("%Y-%m-%d")+'】'+'--德坤深圳机场山东项目单票投保','您好'+ '<br/>' +'客户投保信息如下起保日期【'+datetime.datetime.now().strftime("%Y-%m-%d")+'】'+'请出具保单。'+'<br/>'+'<br/>' + html)
            sql = "update RemoteData set ExceptionStatus = '已写入' where deliveryOrderId = '深圳机场山东项目'"
            cursor.execute(sql)   #执行sql语句
            conn.commit() #提交       
        # w_excel(data1)
    except Exception as err:
        traceback.print_exc()
        print("请求失败",err) 
        sendAlertMail(('manman.zhang@dragonins.com'),'德坤发送失败，请及时处理',str(err))
if __name__ == "__main__":
    getDKRemoteDataToEmail()


         

