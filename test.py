import pandas as pd
import pymssql
import datetime
import traceback
import smtplib
from email.header import Header
from email.mime.text import MIMEText
import pymysql
import math
import pymssql
import datetime
import traceback
import smtplib
from email.header import Header
from email.mime.text import MIMEText
import pymysql

conn = pymysql.connect(host="124.70.184.203",user="root",password="7rus7U5!",database="cmal",charset='utf8')
cursor = conn.cursor() 

sql="select guid,city,license,deliverydate from dispatchavavehicle2 where city like '%,%';"
cursor.execute(sql)
data = cursor.fetchall()
for row in data:
    guid=str(row[0])
    city=str(row[1])
    sql2="select distinct code2 from linepackage where city in ('"+city.replace(",","','")+"');"
    cursor.execute(sql2)
    data2 = cursor.fetchall()
    if len(data2)>1:
        print(guid)

results=[]
for row in data:
    guid=str(row[0])
    city=str(row[1])
    licenseid=str(row[2])
    deliverydate=str(row[3])
    sql2="select city,count(1) from dispatchbills where license like '%"+licenseid+"%' and cast(deliverydate as date)='"+deliverydate+"' and city in ('"+city.replace(",","','")+"') group by city;"
    cursor.execute(sql2)
    data2 = cursor.fetchall()
    for row2 in data2:
        item=(guid,licenseid,deliverydate,str(row2[0]),str(row2[1]))
        results.append(item)
newData=pd.DataFrame(columns=['guid','licenseid','deliverydate','city','citycount'],data=results)
newData.to_csv(str(datetime.datetime.now().strftime('%Y%m%d%H%M%S'))+'.csv', encoding='gbk', index=False)
        