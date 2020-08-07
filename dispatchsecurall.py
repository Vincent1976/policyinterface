import pymssql
import datetime
import traceback
import smtplib
from email.header import Header
from email.mime.text import MIMEText
import pymysql

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


T='2020/4/5'
conn = pymysql.connect(host="124.70.184.203",user="root",password="7rus7U5!",database="cmal",charset='utf8')
cursor = conn.cursor() 

log_file = open('logs/' + datetime.datetime.now().strftime("%Y%m%d%H%M%S%f") +'_dispatchCheck.log',mode='a', encoding='utf-8')

#约束条件1.3：可配载城市数量约束
def CityCheck(T):
    log_file.write('---------------------------可配载城市数量约束 ' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '---------------------------\n')

    sql="select licenseid,COUNT(distinct dispatchbills.city) as 'citycount',dispatchno from dispatchsecurall \
        LEFT JOIN (select * from dispatchbills where cast(deliverydate as date)='"+T+"') dispatchbills on dispatchsecurall.licenseid=SUBSTRING(dispatchbills.license,9) \
        where cast(dispatchsecurall.deliverydate as date)='"+T+"' GROUP BY licenseid,dispatchno order by dispatchno,licenseid"
    cursor.execute(sql)
    data = cursor.fetchall()
    for row in data:
        #板车车牌
        licenseid=str(row[0])
        #板车实际配载城市数
        citycount=int(row[1])
        #调度单号
        dispatchno=str(row[2])

        sql2="select IFNULL(MAX(DATEDIFF(dispatchsecurall.deliverydate,dispatchbills.plandate)),'') as 'citycount' from dispatchbills \
             LEFT JOIN (select * from dispatchsecurall where licenseid='"+licenseid+"' and cast(dispatchsecurall.deliverydate as date)='"+T+"') dispatchsecurall on dispatchsecurall.licenseid=SUBSTRING(dispatchbills.license,9) \
             where SUBSTRING(dispatchbills.license,9)='"+licenseid+"' and cast(dispatchbills.deliverydate as date)='"+T+"'"
        cursor.execute(sql2)
        data2 = cursor.fetchall()
        if len(data2)==0 or str(data2[0][0])=='':
            log_file.write('可配载城市数为空,调度单号：'+str(dispatchno)+',车牌号：'+str(licenseid)+',板车实际配载城市数：'+str(citycount)+'\n')
            continue

        taddx=int(data2[0][0])
        taddxCK=int(data2[0][0])
        if taddx>=0 and taddx<=2:
            taddxCK=2
        elif taddx>=3 and taddx<=4:
            taddxCK=3
        elif taddx>=5 and taddx<=7:
            taddxCK=4
        else:
            log_file.write('可配载城市数量超标,调度单号：'+str(dispatchno)+',车牌号：'+str(licenseid)+',板车实际配载城市数：'+str(citycount)+',可配载城市数(x='+str(taddx)+')：'+str(taddxCK)+'\n')
            continue
        if citycount>taddxCK:
            log_file.write('可配载城市数量超标,调度单号：'+str(dispatchno)+',车牌号：'+str(licenseid)+',板车实际配载城市数：'+str(citycount)+',可配载城市数(x='+str(taddx)+')：'+str(taddxCK)+'\n')
            continue

#约束条件1.4：可配载经销商数量约束
def AddressCheck(T):
    log_file.write('\n\n---------------------------可配载经销商数量约束 ' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '---------------------------\n')

    sql="select licenseid,COUNT(distinct dispatchbills.address) as 'addresscount',dispatchno from dispatchsecurall \
        LEFT JOIN (select * from dispatchbills where cast(deliverydate as date)='"+T+"') dispatchbills on dispatchsecurall.licenseid=SUBSTRING(dispatchbills.license,9) \
        where cast(dispatchsecurall.deliverydate as date)='"+T+"' GROUP BY licenseid,dispatchno order by dispatchno,licenseid"
    cursor.execute(sql)
    data = cursor.fetchall()
    for row in data:
        #板车车牌
        licenseid=str(row[0])
        #板车实际配载经销商数
        addresscount=int(row[1])
        #调度单号
        dispatchno=str(row[2])

        sql2="select IFNULL(MAX(DATEDIFF(dispatchsecurall.deliverydate,dispatchbills.plandate)),'') as 'addresscount' from dispatchbills \
             LEFT JOIN (select * from dispatchsecurall where licenseid='"+licenseid+"' and cast(dispatchsecurall.deliverydate as date)='"+T+"') dispatchsecurall on dispatchsecurall.licenseid=SUBSTRING(dispatchbills.license,9) \
             where SUBSTRING(dispatchbills.license,9)='"+licenseid+"' and cast(dispatchbills.deliverydate as date)='"+T+"'"
        cursor.execute(sql2)
        data2 = cursor.fetchall()
        if len(data2)==0 or str(data2[0][0])=='':
            log_file.write('可配载城市数为空,调度单号：'+str(dispatchno)+',车牌号：'+str(licenseid)+',板车实际配载城市数：'+str(addresscount)+'\n')
            continue

        taddx=int(data2[0][0])
        taddxCK=int(data2[0][0])
        if taddx>=0 and taddx<=2:
            taddxCK=2
        elif taddx>=3 and taddx<=4:
            taddxCK=3
        elif taddx>=5 and taddx<=7:
            taddxCK=4
        else:
            log_file.write('可配载城市数量超标,调度单号：'+str(dispatchno)+',车牌号：'+str(licenseid)+',板车实际配载经销商数：'+str(addresscount)+',可配载经销商数(x='+str(taddx)+')：'+str(taddxCK)+'\n')
            continue
        if addresscount>taddxCK:
            log_file.write('可配载城市数量超标,调度单号：'+str(dispatchno)+',车牌号：'+str(licenseid)+',板车实际配载经销商数：'+str(addresscount)+',可配载经销商数(x='+str(taddx)+')：'+str(taddxCK)+'\n')
            continue

#约束条件1.5：始发地之间拼车
def vlimit(T):
    log_file.write('\n\n---------------------------始发地之间拼车 ' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '---------------------------\n')

    sql="select license,vbase,vlimits from (\
         select *,\
         (select GROUP_CONCAT(distinct vlimit) from dispatchbills where SUBSTRING(dispatchbills.license,9)=dispatchavavehicle2.license and LEFT(dispatchbills.deliverydate,10)=DATE_FORMAT(dispatchavavehicle2.deliverydate,'%Y-%m-%d') GROUP BY right(license,7)) 'vlimits',\
         (select count(distinct vlimit) from dispatchbills where SUBSTRING(dispatchbills.license,9)=dispatchavavehicle2.license and LEFT(dispatchbills.deliverydate,10)=DATE_FORMAT(dispatchavavehicle2.deliverydate,'%Y-%m-%d') GROUP BY right(license,7)) 'vlimitsC' \
         from dispatchavavehicle2 where cast(deliverydate as date)='"+T+"'\
         ) dispatchavavehicle2 where dispatchavavehicle2.vlimitsC>1;"
    cursor.execute(sql)
    data = cursor.fetchall()

    cq='CJY,C01,CJC,NYD,SYB,NYB'
    hb='CQC,KHB'
    for row in data:
        licenseid=str(row[0])
        vbase=str(row[1])
        vlimits=str(row[2])
        list=vlimits.split(',')
        if vbase=='重庆基地':
            bool=True
            for item in list:
                if cq.find(str(item))<0:
                    bool=False
            if bool==False:
                log_file.write('发车地拼库错误,基地：'+str(vbase)+',车牌号：'+str(licenseid)+',出发地拼车'+str(vlimits)+'\n')
                continue
        if vbase=='河北基地':
            bool=True
            for item in list:
                if hb.find(str(item))<0:
                    bool=False
            if bool==False:
                log_file.write('发车地拼库错误,基地：'+str(vbase)+',车牌号：'+str(licenseid)+',出发地拼车'+str(vlimits)+'\n')
                continue

#约束条件1.6：订单分配策略
def SalesidCheck(T):
    log_file.write('\n\n---------------------------订单分配策略 ' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '---------------------------\n')

    sql=" select distinct dispatchbills.salesid from dispatchsecurall left join dispatchbills on dispatchsecurall.orderid=dispatchbills.guid where cast(dispatchsecurall.deliverydate as date)='"+T+"'"
    cursor.execute(sql)
    data = cursor.fetchall()

    for row in data:
        salesid=str(row[0])
        sql=" select count(1) from (\
            select * from dispatchbills where salesid='"+salesid+"' and cast(deliverydate as date)='"+T+"') a left join dispatchsecurall on a.guid=dispatchsecurall.orderid and cast(dispatchsecurall.deliverydate as date)='"+T+"' \
            where IFNULL(dispatchsecurall.guid,'')=''"
        cursor.execute(sql)
        data2 = cursor.fetchall()
        if int(data2[0][0])>0:
            log_file.write('同一订单当日未全部发运,商品订单号：'+str(salesid)+'\n')
            continue

if __name__ == "__main__":
    try:
        CityCheck(T)
        AddressCheck(T)
        print('-----约束检查已完成，请查看结果文件')
    except Exception as err:
        traceback.print_exc()
        print("请求失败",err) 
        sendAlertMail(['qian.hong@dragonins.com','manman.zhang@dragonins.com'],'调度检验出错',str(err)+'<br />')