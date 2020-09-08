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

conn = pymysql.connect(host="124.70.184.203",user="root",password="7rus7U5!",database="cmal",charset='utf8')
cursor = conn.cursor() 

#约束条件1.2：订单下发时间越早的订单越早安排
def DataCheck(T):
    log_file.write('---------------------------订单下发时间越早的订单越早安排 ' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '---------------------------\n')

    sql="select salesid,dispatchno,IFNULL(MAX(DATEDIFF(dispatchsecurall2.deliverydate,dispatchbills.plandate)),0) t from dispatchsecurall2 \
        LEFT JOIN dispatchbills on dispatchsecurall2.orderid=dispatchbills.guid \
		where cast(dispatchsecurall2.deliverydate as date)='"+T+"' GROUP BY salesid,dispatchno order by dispatchno,salesid"
    cursor.execute(sql)
    data = cursor.fetchall()
    for row in data:
        salesid=str(row[0])
        dispatchno=str(row[1])
        tt=int(row[2])
        if tt<=7 and tt>3:
            log_file.write('无法在3日内配载,调度单号：'+str(dispatchno)+',商品订单号：'+str(salesid)+',配载日期差：'+str(tt)+'\n')
            continue
    for row in data:
        salesid=str(row[0])
        dispatchno=str(row[1])
        tt=int(row[2])
        if tt>7:
            log_file.write('无法在7日内配载,调度单号：'+str(dispatchno)+',商品订单号：'+str(salesid)+',配载日期差：'+str(tt)+'\n')
            continue

#约束条件1.3：可配载城市数量约束
def CityCheck(T):
    log_file.write('\n\n---------------------------可配载城市数量约束 ' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '---------------------------\n')

    sql="select licenseid,COUNT(distinct dispatchbills.city) as 'citycount',dispatchno from dispatchsecurall2 \
        LEFT JOIN dispatchbills on dispatchsecurall2.orderid=dispatchbills.guid \
        where cast(dispatchsecurall2.deliverydate as date)='"+T+"' GROUP BY licenseid,dispatchno order by dispatchno,licenseid"
    cursor.execute(sql)
    data = cursor.fetchall()
    for row in data:
        #板车车牌
        licenseid=str(row[0])
        #板车实际配载城市数
        citycount=int(row[1])
        #调度单号
        dispatchno=str(row[2])

        sql2="select IFNULL(MAX(DATEDIFF(dispatchsecurall2.deliverydate,dispatchbills.plandate)),'') as 'citycount' from dispatchbills \
             LEFT JOIN dispatchsecurall2 on dispatchsecurall2.orderid=dispatchbills.guid \
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
        elif taddx>7:
           taddxCK=5
        else:
            log_file.write('可配载城市数量超标,调度单号：'+str(dispatchno)+',车牌号：'+str(licenseid)+',板车实际配载城市数：'+str(citycount)+',可配载城市数(x='+str(taddx)+')：'+str(taddxCK)+'\n')
            continue
        if citycount>taddxCK:
            log_file.write('可配载城市数量超标,调度单号：'+str(dispatchno)+',车牌号：'+str(licenseid)+',板车实际配载城市数：'+str(citycount)+',可配载城市数(x='+str(taddx)+')：'+str(taddxCK)+'\n')
            continue

#约束条件1.4：可配载经销商数量约束
def AddressCheck(T):
    log_file.write('\n\n---------------------------可配载经销商数量约束 ' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '---------------------------\n')

    sql="select licenseid,COUNT(distinct dispatchbills.address) as 'addresscount',dispatchno from dispatchsecurall2 \
        LEFT JOIN dispatchbills on dispatchsecurall2.orderid=dispatchbills.guid \
        where cast(dispatchsecurall2.deliverydate as date)='"+T+"' GROUP BY licenseid,dispatchno order by dispatchno,licenseid"
    cursor.execute(sql)
    data = cursor.fetchall()
    for row in data:
        #板车车牌
        licenseid=str(row[0])
        #板车实际配载经销商数
        addresscount=int(row[1])
        #调度单号
        dispatchno=str(row[2])

        sql2="select IFNULL(MAX(DATEDIFF(dispatchsecurall2.deliverydate,dispatchbills.plandate)),'') as 'addresscount' from dispatchbills \
             LEFT JOIN dispatchsecurall2 on dispatchsecurall2.orderid=dispatchbills.guid \
             where SUBSTRING(dispatchbills.license,9)='"+licenseid+"' and cast(dispatchbills.deliverydate as date)='"+T+"'"
        cursor.execute(sql2)
        data2 = cursor.fetchall()
        if len(data2)==0 or str(data2[0][0])=='':
            log_file.write('可配载经销商数量为空,调度单号：'+str(dispatchno)+',车牌号：'+str(licenseid)+',板车实际配载城市数：'+str(addresscount)+'\n')
            continue

        taddx=int(data2[0][0])
        taddxCK=int(data2[0][0])
        if taddx>=0 and taddx<=2:
            taddxCK=3
        elif taddx>=3 and taddx<=4:
            taddxCK=4
        elif taddx>=5 and taddx<=7:
            taddxCK=5
        elif taddx>7:
            taddxCK=6
        else:
            log_file.write('可配载经销商数量超标,调度单号：'+str(dispatchno)+',车牌号：'+str(licenseid)+',板车实际配载经销商数：'+str(addresscount)+',可配载经销商数(x='+str(taddx)+')：'+str(taddxCK)+'\n')
            continue
        if addresscount>taddxCK:
            log_file.write('可配载经销商数量超标,调度单号：'+str(dispatchno)+',车牌号：'+str(licenseid)+',板车实际配载经销商数：'+str(addresscount)+',可配载经销商数(x='+str(taddx)+')：'+str(taddxCK)+'\n')
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

    sql="select dispatchbills.salesid,dispatchsecurall2.dispatchno from dispatchsecurall2 left join dispatchbills on dispatchsecurall2.orderid=dispatchbills.guid \
        where cast(dispatchsecurall2.deliverydate as date)='"+T+"' group by dispatchbills.salesid,dispatchsecurall2.dispatchno order by dispatchsecurall2.dispatchno"
    cursor.execute(sql)
    data = cursor.fetchall()
    for row in data:
        salesid=str(row[0])
        dispatchno=str(row[1])
        sql=" select count(1) from (\
            select * from dispatchbills where salesid='"+salesid+"' and cast(deliverydate as date)='"+T+"') a left join dispatchsecurall2 on a.guid=dispatchsecurall2.orderid and cast(dispatchsecurall2.deliverydate as date)='"+T+"' \
            where IFNULL(dispatchsecurall2.guid,'')=''"
        cursor.execute(sql)
        data2 = cursor.fetchall()
        if int(data2[0][0])>0:
            log_file.write('同一订单当日未全部发运,调度单号：'+dispatchno+',商品订单号：'+str(salesid)+'\n')
            continue

    sql="select dispatchbills.salesid,dispatchsecurall2.dispatchno,GROUP_CONCAT(distinct dispatchsecurall2.licenseid) from dispatchsecurall2 \
        left join dispatchbills on dispatchsecurall2.orderid=dispatchbills.guid where cast(dispatchsecurall2.deliverydate as date)='"+T+"' \
        GROUP BY dispatchbills.salesid,dispatchsecurall2.dispatchno HAVING count(distinct dispatchsecurall2.licenseid)>1"
    cursor.execute(sql)
    data2 = cursor.fetchall()
    if len(data2)>0:
        log_file.write('\n\n---------------------------一张订单包含多辆商品车且装载在多辆板车上的情形---------------------------\n')
        for row in data2:
            salesid=str(row[0])
            dispatchno=str(row[1])
            count=str(row[2])
            log_file.write('订单'+str(salesid)+'包含多辆商品车且装载在多辆板车上的情形,调度单号：'+dispatchno+',车,装载板车数：'+str(count)+'\n')

#约束条件2.1：承运车辆（板车）装载数量
def VcountCheck(T):
    log_file.write('\n\n---------------------------承运车辆（板车）装载数量 ' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '---------------------------\n')

    sql="select dispatchsecurall2.licenseid,dispatchsecurall2.dispatchno,count(1) from dispatchsecurall2 \
         where cast(dispatchsecurall2.deliverydate as date)='"+T+"' GROUP BY dispatchsecurall2.licenseid,dispatchsecurall2.dispatchno order by dispatchsecurall2.dispatchno;"
    cursor.execute(sql)
    data = cursor.fetchall()

    for row in data:
        licenseid=str(row[0])
        dispatchno=str(row[1])
        count=int(row[2])
        sql="select IFNULL(vcount,'') from dispatchavavehicle2 where license='"+licenseid+"' and cast(deliverydate as date)='"+T+"'"
        cursor.execute(sql)
        data2 = cursor.fetchall()
        if len(data2)==0 or str(data2[0][0])=='':
            log_file.write('该车无可用车位数 ,调度单号：'+dispatchno+'，车牌号：'+licenseid+',板车配载车辆数：'+str(count)+',可用车位数：无\n')
            continue
        if count<int(data2[0][0]):
            log_file.write('可用车辆未满载,调度单号：'+dispatchno+'，车牌号：'+licenseid+',板车配载车辆数：'+str(count)+',可用车位数：'+str(data2[0][0])+'\n')
            continue
        sql="select IFNULL(loadcount,'') from dispatchvehicle where licenseid='"+licenseid+"'"
        cursor.execute(sql)
        data3 = cursor.fetchall()
        if len(data3)==0 or str(data3[0][0])=='':
            log_file.write('该车无可用车位数 ,调度单号：'+dispatchno+'，车牌号：'+licenseid+',板车配载车辆数：'+str(count)+',可用车位数：无\n')
            continue
        if count>int(data3[0][0]):
            log_file.write('超过可用车位数,调度单号：'+dispatchno+'，车牌号：'+licenseid+',板车配载车辆数：'+str(count)+',可用车位数：'+str(data3[0][0])+'\n')
            continue

#约束条件3.3：承运商线路包及其份额（+当日发运数）
def LinesCheck(T):
    log_file.write('\n\n---------------------------未匹配线路包 ' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '---------------------------\n')

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
        log_file.write('未匹配线路包，调度单号：'+dispatchno+'，车牌号：'+licenseid+'，承运商：'+cys+',城市："'+city+'"\n')


    log_file.write('\n\n---------------------------不在同一个线路包内 ' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '---------------------------\n')

    sql="select distinct dispatchsecurall2.cys,dispatchno,dispatchsecurall2.licenseid from dispatchsecurall2 where cast(dispatchsecurall2.deliverydate as date)='"+T+"';"
    cursor.execute(sql)
    data = cursor.fetchall()
    for row in data:
        cys=str(row[0])
        dispatchno=str(row[1])
        licenseid=str(row[2])

        # sql=" select IFNULL(GROUP_CONCAT(code2),''),IFNULL(GROUP_CONCAT(city),'') from linepackage LEFT JOIN linepackage_cys on linepackage_cys.code=code2 where cys='"+cys+"' \
        #     and city in (select distinct city from dispatchsecurall2 where cys='"+cys+"' and dispatchno='"+dispatchno+"' and licenseid='"+licenseid+"') GROUP BY cys"
        sql="select IFNULL(GROUP_CONCAT(code2),'') 'code2',IFNULL(GROUP_CONCAT(city),'') 'city',vount from (\
 select IFNULL(GROUP_CONCAT(code2),'') 'code2',IFNULL(GROUP_CONCAT(city),'') 'city','1' vount from linepackage LEFT JOIN linepackage_cys on linepackage_cys.code=code2 where cys='"+cys+"' \
 and city in (select distinct city from dispatchsecurall2 where cys='"+cys+"' and dispatchno='"+dispatchno+"' and licenseid='"+licenseid+"') GROUP BY code2) linepackage GROUP BY vount HAVING count(1)>1;"
        cursor.execute(sql)
        data2 = cursor.fetchall()
        for row in data2:
            code2=str(row[0])
            city=str(row[1])
            log_file.write('不在同一个线路包内，调度单号：'+dispatchno+'，车牌号：'+licenseid+'，承运商：'+cys+',城市："'+city+'",线路包code：'+code2+'\n')


    log_file.write('\n\n---------------------------承运商线路包及其份额 ' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '---------------------------\n')

    log_file.write('---------------------------模型前---------------------------\n')
    sql="select code,cys,cysshare from linepackage_cys"
    cursor.execute(sql)
    data = cursor.fetchall()
    for row in data:
        code=str(row[0])
        cys=str(row[1])
        cysshare=float(row[2])
        
        sql="select IFNULL(GROUP_CONCAT(city),'') from linepackage where code2='"+code+"' GROUP BY code2;"
        cursor.execute(sql)
        data_city = cursor.fetchall()

        sql="select IFNULL(count(1),'') from dispatchsecurall2 where cys='"+cys+"' and city in (select city from linepackage where code2='"+code+"') \
            and cast(dispatchsecurall2.deliverydate as date)<='"+T+"' and dispatchno like '%r';"
        cursor.execute(sql)
        data_fz = cursor.fetchall()
        sql="select IFNULL(count(1),'') from dispatchsecurall2 where city in (select city from linepackage where code2='"+code+"') \
            and cast(dispatchsecurall2.deliverydate as date)<='"+T+"' and dispatchno like '%r';"
        cursor.execute(sql)
        data_fm = cursor.fetchall()
        sql="select IFNULL(count(1),'') from dispatchsecurall2 where cys='"+cys+"' and city in (select city from linepackage where code2='"+code+"') \
            and cast(dispatchsecurall2.deliverydate as date)='"+T+"' and dispatchno like '%r';"
        cursor.execute(sql)
        data_now = cursor.fetchall()

        count_fz=0
        if len(data_fz)>0 and str(data_fz[0][0])!='':
            count_fz=float(data_fz[0][0])
        count_fm=0
        if len(data_fm)>0 and str(data_fm[0][0])!='':
            count_fm=float(data_fm[0][0])
        count_now=0
        if len(data_now)>0 and str(data_now[0][0])!='':
            count_now=float(data_now[0][0])
        if count_fm==0:
            result=0
        else:
            result=round(float(count_fz/count_fm),2)
        cha=round(float(cysshare-float(result)),2)
        log_file.write('承运商线路包及其份额，承运商：'+cys+',城市："'+str(data_city[0][0])+'"，调度后线路包执行比例：'+str(cysshare)+',分子：'+str(count_fz)+',分母：'+str(count_fm)+'，shipper占比：'+str(result)+',占比差：'+str(cha)+',当日发运数：'+str(count_now)+'\n')

    log_file.write('---------------------------模型后---------------------------\n')
    for row in data:
        code=str(row[0])
        cys=str(row[1])
        cysshare=float(row[2])

        sql="select IFNULL(GROUP_CONCAT(city),'') from linepackage where code2='"+code+"' GROUP BY code2;"
        cursor.execute(sql)
        data_city = cursor.fetchall()
        
        sql="select IFNULL(count(1),'') from dispatchsecurall2 where cys='"+cys+"' and city in (select city from linepackage where code2='"+code+"') \
            and cast(dispatchsecurall2.deliverydate as date)<='"+T+"' and dispatchno like '%ai';"
        cursor.execute(sql)
        data_fz2 = cursor.fetchall()
        sql="select IFNULL(count(1),'') from dispatchsecurall2 where city in (select city from linepackage where code2='"+code+"') \
            and cast(dispatchsecurall2.deliverydate as date)<='"+T+"' and dispatchno like '%ai';"
        cursor.execute(sql)
        data_fm2 = cursor.fetchall()
        sql="select IFNULL(count(1),'') from dispatchsecurall2 where cys='"+cys+"' and city in (select city from linepackage where code2='"+code+"') \
            and cast(dispatchsecurall2.deliverydate as date)='"+T+"' and dispatchno like '%ai';"
        cursor.execute(sql)
        data_now2 = cursor.fetchall()

        count_fz2=0
        if len(data_fz2)>0 and str(data_fz2[0][0])!='':
            count_fz2=float(data_fz2[0][0])
        count_fm2=0
        if len(data_fm2)>0 and str(data_fm2[0][0])!='':
            count_fm2=float(data_fm[0][0])
        count_now2=0
        if len(data_now2)>0 and str(data_now2[0][0])!='':
            count_now2=float(data_now2[0][0])
        if count_fm2==0:
            result2=0
        else:
            result2=round(float(count_fz2/count_fm2),2)
        cha2=round(float(cysshare-float(result2)),2)
        log_file.write('承运商线路包及其份额，承运商：'+cys+',城市：“'+str(data_city[0][0])+'”，调度后线路包执行比例：'+str(cysshare)+',分子：'+str(count_fz2)+',分母：'+str(count_fm2)+'，shipper占比：'+str(result2)+',占比差：'+str(cha2)+',当日发运数：'+str(count_now2)+'\n')

if __name__ == "__main__":
    try:
        start=datetime.datetime.strptime('2020/4/5','%Y/%m/%d')
        end=datetime.datetime.strptime('2020/4/7','%Y/%m/%d')
        
        while start<=end:
            T=str(start.strftime('%Y/%m/%d'))

            log_file = open('logs/' + T.replace('/','_') +'_'+datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")+'.log',mode='a', encoding='utf-8')
            log_file.write('---------------------------T = '+T+'---------------------------\n\n')

            DataCheck(T)
            CityCheck(T)
            AddressCheck(T)
            vlimit(T)
            SalesidCheck(T)
            VcountCheck(T)
            LinesCheck(T)

            start+=datetime.timedelta(days=1)

        print('-----约束检查已完成，请查看结果文件')
    except Exception as err:
        traceback.print_exc()
        print("请求失败",err) 
        sendAlertMail(['qian.hong@dragonins.com','manman.zhang@dragonins.com'],'调度检验出错',str(err)+'<br />')