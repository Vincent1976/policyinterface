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

conn = pymssql.connect(host="121.36.204.186",port = "15343",user="sa",password="sate1llite",database="newInsurance",charset='utf8')
cursor = conn.cursor() #创建一个游标对象，python 里的sql 语句都要通过cursor 来执行
# 每日定时获取德坤前日所有投保数据发邮件给保险公司
def getDKRemoteDataToEmail(file_excel):
    try:
        # 打开数据库连接
        sql = "select custCoName,insuredName, channelOrderId,licenseId,trafficType,packageType,cargoType,cargoName,departProvince,departCity,departDateTime,cargoCount,deliveryAddress,policyRate from RemoteData where deliveryOrderId='深圳机场山东项目'and policyNo<>'TEST' and ExceptionStatus <> '已写入'" 
        cursor.execute(sql)   #执行sql语句
        data = cursor.fetchall()  #读取查询结果
        data1 =[]
        for row in data:
            pams = ('深圳德坤物流有限公司及其下属分公司，子公司及控股公司', '实际货主',row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11],row[12],row[13])        
            data1.append(pams)
        w_excel(data1)
    except Exception as err:
        traceback.print_exc()
        print("请求失败",err) 
        sendAlertMail(('manman.zhang@dragonins.com'),'德坤发送失败，请及时处理',str(err))
#操作excel
def w_excel(data):
    book = xlwt.Workbook() #新建一个excel
    sheet = book.add_sheet(datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")+'dkexcel') #新建一个sheet页
    title = ['custCoName','insuredName','channelOrderId','licenseId','trafficType','packageType','cargoType','cargoName','departProvince','departCity','departDateTime','cargoCount','deliveryAddress','policyRate']
    #写表头
    i = 0
    for header in title:
        sheet.write(0,i,header)
        i+=1
        
#写入数据
    for row in range(1,len(data)):
        for col in range(0,len(data[row])):
            sheet.write(row,col,data[row][col])
        row+=1
    col+=1
    filename = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")
    book.save("D:\\AutoDeclareToZA_QL\\PolicyFiles\\" + datetime.datetime.now().strftime("%Y%m%d%H%M%S%f") + 'dkexcel' + '.xls')
    sql = "update RemoteData set ExceptionStatus = '已写入' where deliveryOrderId = '深圳机场山东项目'"
    cursor.execute(sql)   #执行sql语句
    conn.commit() #提交
    sendAlertMail(('manman.zhang@dragonins.com'),'德坤运输清单已发送，请点击下方的链接进行下载文件','http://121.36.193.132:8088/'+filename + 'dkexcel' + '.xls')
    print("导出成功！")
if __name__ == "__main__":
    file_excel = r"E:\dklog\1.xls"
    getDKRemoteDataToEmail(file_excel)


         

