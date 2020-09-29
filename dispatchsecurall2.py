import pandas as pd
import datetime
import traceback
import smtplib
from email.header import Header
from email.mime.text import MIMEText
import pymysql
import math
import sys
import datetime

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

conn = pymysql.connect(host="124.70.184.203",user="root",password="7rus7U5!",database="cmal",charset='utf8')
cursor = conn.cursor() 
results=[]

#约束条件1.2：订单下发时间越早的订单越早安排
def DataCheck(T,tbname):
    sql="select licenseid,dispatchno from dispatchsecurall2 where cast(dispatchsecurall2.deliverydate as date)='"+T+"' GROUP BY licenseid,dispatchno order by dispatchno,licenseid"
    cursor.execute(sql)
    data = cursor.fetchall()
    for row in data:
        licenseid=str(row[0])
        dispatchno=str(row[1])
        item=[]

        sql2="select IFNULL(MAX(cast(timestampdiff(HOUR,dispatchbills.plandate,dispatchbills.deliverydate) as decimal)/24),'') as 'count' from dispatchbills \
             LEFT JOIN dispatchsecurall2 on dispatchsecurall2.orderid=dispatchbills.guid \
             where dispatchsecurall2.licenseid='"+licenseid+"' and dispatchsecurall2.dispatchno='"+dispatchno+"'"
        if dispatchno.find('ai')>=0:
            sql2="select IFNULL(MAX(cast(timestampdiff(HOUR,dispatchbills.plandate,dispatchsecurall2.deliverydate) as decimal)/24),'') as 'count' from dispatchbills \
                 LEFT JOIN dispatchsecurall2 on dispatchsecurall2.orderid=dispatchbills.guid \
                 where dispatchsecurall2.licenseid='"+licenseid+"' and dispatchsecurall2.dispatchno='"+dispatchno+"'"
        cursor.execute(sql2)
        data2 = cursor.fetchall()
        if len(data2)==0 or str(data2[0][0])=='':
            continue
        tt=int(float(data2[0][0]))
        if tt<=7 and tt>3:
            item.extend(['约束条件1.2：订单下发时间越早的订单越早安排',str(dispatchno),'',str(tt),str(licenseid),'','','','','','','','','','','','无法在3日内配载',str(T)])
            results.append(item)
            continue
        if tt>7:
            item.extend(['约束条件1.2：订单下发时间越早的订单越早安排',str(dispatchno),'',str(tt),str(licenseid),'','','','','','','','','','','','无法在7日内配载',str(T)])
            results.append(item)
            continue

#约束条件1.3：可配载城市数量约束
def CityCheck(T,tbname):
    sql="select licenseid,COUNT(distinct dispatchbills.city) as 'citycount',dispatchno from dispatchsecurall2 \
        LEFT JOIN dispatchbills on dispatchsecurall2.orderid=dispatchbills.guid \
        where cast(dispatchsecurall2.deliverydate as date)='"+T+"' GROUP BY licenseid,dispatchno order by dispatchno,licenseid"
    cursor.execute(sql)
    data = cursor.fetchall()
    for row in data:
        licenseid=str(row[0])
        citycount=int(row[1])
        dispatchno=str(row[2])
        item=[]
        
        sql2="select IFNULL(MAX(cast(timestampdiff(HOUR,dispatchbills.plandate,dispatchbills.deliverydate) as decimal)/24),'') as 'citycount' from dispatchbills \
             LEFT JOIN dispatchsecurall2 on dispatchsecurall2.orderid=dispatchbills.guid \
             where dispatchsecurall2.licenseid='"+licenseid+"' and dispatchsecurall2.dispatchno='"+dispatchno+"'"
        if dispatchno.find('ai')>=0:
            sql2="select IFNULL(MAX(cast(timestampdiff(HOUR,dispatchbills.plandate,dispatchsecurall2.deliverydate) as decimal)/24),'') as 'citycount' from dispatchbills \
                 LEFT JOIN dispatchsecurall2 on dispatchsecurall2.orderid=dispatchbills.guid \
                 where dispatchsecurall2.licenseid='"+licenseid+"' and dispatchsecurall2.dispatchno='"+dispatchno+"'"
        cursor.execute(sql2)
        data2 = cursor.fetchall()
        if len(data2)==0 or str(data2[0][0])=='':
            item.extend(['约束条件1.3：可配载城市数量约束',str(dispatchno),'','',str(licenseid),'','','',str(citycount),'','','','','','','','可配载城市数为空',str(T)])
            results.append(item)
            continue

        # taddx=math.ceil(float(data2[0][0]))
        taddx=int(float(data2[0][0]))
        taddxCK=math.ceil(float(data2[0][0]))
        if taddx>=0 and taddx<=2:
            taddxCK=2
        elif taddx>=3 and taddx<=4:
            taddxCK=3
        elif taddx>=5 and taddx<=7:
            taddxCK=4
        elif taddx>7:
           taddxCK=5
        else:
            item.extend(['约束条件1.3：可配载城市数量约束',str(dispatchno),'','',str(licenseid),'','','',str(citycount),str(taddx),str(taddxCK),'','','','','','可配载城市数量未配置',str(T)])
            results.append(item)
            continue
        if citycount>taddxCK:
            item.extend(['约束条件1.3：可配载城市数量约束',str(dispatchno),'','',str(licenseid),'','','',str(citycount),str(taddx),str(taddxCK),'','','','','','可配载城市数量超标',str(T)])
            results.append(item)
            continue

#约束条件1.4：可配载经销商数量约束
def AddressCheck(T,tbname):
    sql="select licenseid,COUNT(distinct dispatchbills.address) as 'addresscount',dispatchno from dispatchsecurall2 \
        LEFT JOIN dispatchbills on dispatchsecurall2.orderid=dispatchbills.guid \
        where cast(dispatchsecurall2.deliverydate as date)='"+T+"' GROUP BY licenseid,dispatchno order by dispatchno,licenseid"
    cursor.execute(sql)
    data = cursor.fetchall()
    for row in data:
        licenseid=str(row[0])
        addresscount=int(row[1])
        dispatchno=str(row[2])
        item=[]

        sql2="select IFNULL(MAX(cast(timestampdiff(HOUR,dispatchbills.plandate,dispatchbills.deliverydate) as decimal)/24),'') as 'addresscount' from dispatchbills \
             LEFT JOIN dispatchsecurall2 on dispatchsecurall2.orderid=dispatchbills.guid \
             where dispatchsecurall2.licenseid='"+licenseid+"' and dispatchsecurall2.dispatchno='"+dispatchno+"'"
        if dispatchno.find('ai')>=0:
            sql2="select IFNULL(MAX(cast(timestampdiff(HOUR,dispatchbills.plandate,dispatchsecurall2.deliverydate) as decimal)/24),'') as 'addresscount' from dispatchbills \
                 LEFT JOIN dispatchsecurall2 on dispatchsecurall2.orderid=dispatchbills.guid \
                 where dispatchsecurall2.licenseid='"+licenseid+"' and dispatchsecurall2.dispatchno='"+dispatchno+"'"
        cursor.execute(sql2)
        data2 = cursor.fetchall()
        if len(data2)==0 or str(data2[0][0])=='':
            item.extend(['约束条件1.4：可配载经销商数量约束',str(dispatchno),'','',str(licenseid),'','','',str(addresscount),'','','','','','','','可配载经销商数量为空',str(T)])
            results.append(item)
            continue

        # taddx=math.ceil(float(data2[0][0]))
        taddx=int(float(data2[0][0]))
        taddxCK=math.ceil(float(data2[0][0]))
        if taddx>=0 and taddx<=2:
            taddxCK=3
        elif taddx>=3 and taddx<=4:
            taddxCK=4
        elif taddx>=5 and taddx<=7:
            taddxCK=5
        elif taddx>7:
            taddxCK=6
        else:
            item.extend(['约束条件1.4：可配载经销商数量约束',str(dispatchno),'','',str(licenseid),'','','',str(addresscount),str(taddx),str(taddxCK),'','','','','','可配载经销商数量未配置',str(T)])
            results.append(item)
            continue
        if addresscount>taddxCK:
            item.extend(['约束条件1.4：可配载经销商数量约束',str(dispatchno),'','',str(licenseid),'','','',str(addresscount),str(taddx),str(taddxCK),'','','','','','可配载经销商数量超标',str(T)])
            results.append(item)
            continue

#约束条件1.5：始发地之间拼车
def vlimit(T,tbname):
    sql="select license,vbase,vlimits from (\
         select *,\
         (select GROUP_CONCAT(distinct vlimit) from dispatchbills where SUBSTRING(dispatchbills.license,9)="+tbname+".license and LEFT(dispatchbills.deliverydate,10)=DATE_FORMAT("+tbname+".deliverydate,'%Y-%m-%d') GROUP BY right(license,7)) 'vlimits',\
         (select count(distinct vlimit) from dispatchbills where SUBSTRING(dispatchbills.license,9)="+tbname+".license and LEFT(dispatchbills.deliverydate,10)=DATE_FORMAT("+tbname+".deliverydate,'%Y-%m-%d') GROUP BY right(license,7)) 'vlimitsC' \
         from "+tbname+" where cast(deliverydate as date)='"+T+"'\
         ) "+tbname+" where "+tbname+".vlimitsC>1;"
    cursor.execute(sql)
    data = cursor.fetchall()

    cq='CJY,C01,CJC,NYD,SYB,NYB'
    hb='CQC,KHB'
    for row in data:
        licenseid=str(row[0])
        vbase=str(row[1])
        vlimits=str(row[2])
        item=[]

        list=vlimits.split(',')
        if vbase=='重庆基地':
            bool=True
            for item in list:
                if cq.find(str(item))<0:
                    bool=False
            if bool==False:
                item.extend(['约束条件1.5：始发地之间拼车','','','',str(licenseid),'','','','','','','',str(vbase),str(vlimits),'','','发车地拼库错误',str(T)])
                results.append(item)
                continue
        if vbase=='河北基地':
            bool=True
            for item in list:
                if hb.find(str(item))<0:
                    bool=False
            if bool==False:
                item.extend(['约束条件1.5：始发地之间拼车','','','',str(licenseid),'','','','','','','',str(vbase),str(vlimits),'','','发车地拼库错误',str(T)])
                results.append(item)
                continue

#约束条件1.6：订单分配策略
def SalesidCheck(T,tbname):
    sql="select dispatchbills.salesid,dispatchsecurall2.dispatchno from dispatchsecurall2 left join dispatchbills on dispatchsecurall2.orderid=dispatchbills.guid \
        where cast(dispatchsecurall2.deliverydate as date)='"+T+"' group by dispatchbills.salesid,dispatchsecurall2.dispatchno order by dispatchsecurall2.dispatchno"
    cursor.execute(sql)
    data = cursor.fetchall()
    for row in data:
        salesid=str(row[0])
        dispatchno=str(row[1])
        item=[]

        sql=" select count(1) from (\
            select * from dispatchbills where salesid='"+salesid+"' and cast(deliverydate as date)='"+T+"') a left join dispatchsecurall2 on a.guid=dispatchsecurall2.orderid and cast(dispatchsecurall2.deliverydate as date)='"+T+"' \
            where IFNULL(dispatchsecurall2.guid,'')=''"
        cursor.execute(sql)
        data2 = cursor.fetchall()
        if int(data2[0][0])>0:
            item.extend(['约束条件1.6：订单分配策略',dispatchno,salesid,'','','','','','','','','','','','','','同一订单当日未全部发运',str(T)])
            results.append(item)
            continue

    # sql="select dispatchbills.salesid,dispatchsecurall2.dispatchno,GROUP_CONCAT(distinct dispatchsecurall2.licenseid) from dispatchsecurall2 \
    #     left join dispatchbills on dispatchsecurall2.orderid=dispatchbills.guid where cast(dispatchsecurall2.deliverydate as date)='"+T+"' \
    #     GROUP BY dispatchbills.salesid,dispatchsecurall2.dispatchno HAVING count(distinct dispatchsecurall2.licenseid)>1"
    sql="select * from (\
        select T.*, (select count(1) from dispatchbills WHERE T.salesid=dispatchbills.salesid) 'countB' from (\
        select dispatchno, licenseid, salesid, count(1) 'countA' from dispatchsecurall2 \
        left join dispatchbills on dispatchsecurall2.orderid=dispatchbills.guid where cast(dispatchsecurall2.deliverydate as date)='"+T+"' \
        GROUP BY dispatchno, licenseid, salesid)T\
        ) T2 WHERE countA<countB"
    cursor.execute(sql)
    data2 = cursor.fetchall()
    if len(data2)>0:
        for row in data2:
            dispatchno=str(row[0])
            licenseid=str(row[1])
            salesid=str(row[2])
            countA=str(row[3])
            countB=str(row[4])
            item=[]
            item.extend(['约束条件1.6：订单分配策略',dispatchno,salesid,'',licenseid,'','','','','','','','',countA+','+countB,'','','订单包含多辆商品车且装载在多辆板车上的情形',str(T)])
            results.append(item)

#约束条件2.1：承运车辆（板车）装载数量
def VcountCheck(T,tbname):
    sql="select dispatchsecurall2.licenseid,dispatchsecurall2.dispatchno,count(1) from dispatchsecurall2 \
         where cast(dispatchsecurall2.deliverydate as date)='"+T+"' GROUP BY dispatchsecurall2.licenseid,dispatchsecurall2.dispatchno order by dispatchsecurall2.dispatchno;"
    cursor.execute(sql)
    data = cursor.fetchall()

    for row in data:
        licenseid=str(row[0])
        dispatchno=str(row[1])
        count=int(row[2])
        item=[]

        if dispatchno.find('ai')>=0:
            sql="select IFNULL(vcount,'') from "+tbname+" where guid =(select distinct licenseguid from dispatchsecurall2 where licenseid='"+licenseid+"' and dispatchno='"+dispatchno+"')"
            cursor.execute(sql)
            data2 = cursor.fetchall()
            if len(data2)==0 or str(data2[0][0])=='':
                item.extend(['约束条件2.1：承运车辆（板车）装载数量',dispatchno,'','',licenseid,'','','','','','','','','',str(count),'','"+tbname+"未找到该车',str(T)])
                results.append(item)
                continue
            if count<int(data2[0][0]):
                item.extend(['约束条件2.1：承运车辆（板车）装载数量',dispatchno,'','',licenseid,'','','','','','','','','',str(count),str(data2[0][0]),'可用车辆未满载',str(T)])
                results.append(item)
                continue

        sql="select IFNULL(loadcount,'') from dispatchvehicle where licenseid='"+licenseid+"'"
        cursor.execute(sql)
        data3 = cursor.fetchall()
        if len(data3)==0 or str(data3[0][0])=='':
            item.extend(['约束条件2.1：承运车辆（板车）装载数量',dispatchno,'','',licenseid,'','','','','','','','','',str(count),'','dispatchvehicle未找到该车',str(T)])
            results.append(item)
            continue
        if count>int(data3[0][0]):
            item.extend(['约束条件2.1：承运车辆（板车）装载数量',dispatchno,'','',licenseid,'','','','','','','','','',str(count),str(data3[0][0]),'超过核定车位数',str(T)])
            results.append(item)
            continue

#约束条件3.3：承运商线路包及其份额（+当日发运数）
def LinesCheck(T,tbname):
    sql="select distinct dispatchsecurall2.cys,dispatchsecurall2.city,dispatchno,dispatchsecurall2.licenseid from dispatchsecurall2 \
        left join (select linepackage.*, cys, cysshare from linepackage LEFT JOIN linepackage_cys on linepackage_cys.code=code2) linepackage on linepackage.cys=dispatchsecurall2.cys \
        and linepackage.city=dispatchsecurall2.city where IFNULL(linepackage.guid,'')='' and cast(dispatchsecurall2.deliverydate as date)='"+T+"';"
    cursor.execute(sql)
    data = cursor.fetchall()
    for row in data:
        cys=str(row[0])
        city=str(row[1])
        dispatchno=str(row[2])
        licenseid=str(row[3])
        item=[]
        item.extend(['约束条件3.3：承运商线路包及其份额（+当日发运数）',dispatchno,'','',licenseid,cys,city,'','','','','','','','','','未匹配线路包',str(T)])
        results.append(item)

    sql="select distinct dispatchsecurall2.cys,dispatchno,dispatchsecurall2.licenseid from dispatchsecurall2 where cast(dispatchsecurall2.deliverydate as date)='"+T+"';"
    cursor.execute(sql)
    data = cursor.fetchall()
    for row in data:
        cys=str(row[0])
        dispatchno=str(row[1])
        licenseid=str(row[2])

        sql="select IFNULL(GROUP_CONCAT(code2),'') 'code2',IFNULL(GROUP_CONCAT(city),'') 'city',vount from (\
 select IFNULL(GROUP_CONCAT(code2),'') 'code2',IFNULL(GROUP_CONCAT(city),'') 'city','1' vount from linepackage LEFT JOIN linepackage_cys on linepackage_cys.code=code2 where cys='"+cys+"' \
 and city in (select distinct city from dispatchsecurall2 where cys='"+cys+"' and dispatchno='"+dispatchno+"' and licenseid='"+licenseid+"') GROUP BY code2) linepackage GROUP BY vount HAVING count(1)>1;"
        cursor.execute(sql)
        data2 = cursor.fetchall()
        for row in data2:
            code2=str(row[0])
            code2=code2.replace(",","/")
            city=str(row[1])
            item=[]
            item.extend(['约束条件3.3：承运商线路包及其份额（+当日发运数）',dispatchno,'','',licenseid,cys,city,code2,'','','','','','','','','不在同一个线路包内',str(T)])
            results.append(item)

if __name__ == "__main__":
    try:
        argv1=input("请输入开始时间(例如：2020/4/3)：")
        argv2=input("请输入结束时间(例如：2020/4/10)：")
        argv3=input("请输入可用车辆表(\
0 原始数据（每条一城）\
1 原始数据（每条多城）\
2 加工数据（线路包分离）\
3 加工数据（线路包分离+接单立即发车）)：")

        tbname=""
        if argv3=="0":
            tbname="dispatchavavehicle_origin"
        elif argv3=="1":
            tbname="dispatchavavehicle2_copy"
        elif argv3=="2":
            tbname="dispatchavavehicle2_test3"
        else:
            tbname="dispatchavavehicle2"

        start=datetime.datetime.strptime(argv1,'%Y/%m/%d')
        end=datetime.datetime.strptime(argv2,'%Y/%m/%d')
           
        while start<=end:
            T=str(start.strftime('%Y/%m/%d'))
            DataCheck(T,tbname)
            CityCheck(T,tbname)
            AddressCheck(T,tbname)
            vlimit(T,tbname)
            SalesidCheck(T,tbname)
            VcountCheck(T,tbname)
            LinesCheck(T,tbname)
            start+=datetime.timedelta(days=1)

        newData=pd.DataFrame(columns=['校验约束','调度单号','商品订单号','配载日期差','车牌号','承运商','城市','线路包code','板车实际配载(城市/经销商)数','可配载(城市/经销商)数','可配载(城市/经销商)数x','基地','出发地拼车','装载板车','板车配载车辆数','可用车位数','校验结果','T'],data=results)
        newData.to_csv(str(datetime.datetime.now().strftime('%Y%m%d%H%M%S'))+'.csv', encoding='gbk', index=False)
        print('-----约束检查已完成，请查看结果文件')
    except Exception as err:
        traceback.print_exc()
        print("请求失败",err) 
        sendAlertMail(['qian.hong@dragonins.com','daniel.lee@dragonins.com'],'调度检验出错',str(err)+'<br />')