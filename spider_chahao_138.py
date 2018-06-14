#!/usr/bin/python
# -*- coding: UTF-8 -*-
import re
import bs4
import requests
import pymysql
import time

starttime = time.time()
# url = 'http://www.ip138.com:8080/search.asp'
url = 'https://www.chahaoba.com/'
headers = {'User-Agent': 'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)'}
    # 打开数据库连接
client = pymysql.connect(host='192.153.1.154',
                             user='root',
                             password='100154',
                             port = 50014,
                             db = 'lnsy_bigdata_db',
                             charset = "utf8")

    # 使用 cursor() 方法创建一个游标对象 cursor
cursor = client.cursor()
print ('154数据库连接成功')
#用户输入插入时间
while True:
    start_date = None
    try:
        start_date = input("输入开始时间（格式，'yyyy-mm-dd'）：")
    except:
        pass
    if re.match(r"([']\d{4}-\d{2}-\d{2}['])",start_date):break
while True:
    end_date = None
    try:
        end_date = input("输入结束时间（格式，'yyyy-mm-dd'）：")
    except:
        pass
    if re.match(r"([']\d{4}-\d{2}-\d{2}['])",start_date):break
#提取出数据库中不存在的号段
select_sql = "SELECT DISTINCT(province) province FROM temp_trade_show_user WHERE province NOT IN (SELECT prov FROM db_province_city)"
#更新号段数据库
update_sql = "UPDATE db_province_city SET city = province WHERE city = ''"
#/*将提取的核心网原始数据（辽展），导入temp_trade_show_user*/
replace_sql_lz = """REPLACE INTO temp_trade_show_user
SELECT
DISTINCT(用户号码) user_number,
LEFT(用户号码,7) provience,
终端类型 equipment,
imei,
接入网类型 network,
DATE_FORMAT(结束时间,'%Y-%m-%d') DATETIME,
DATE_FORMAT(结束时间,'%H') SHOUR,
'工业展览馆' trade_show
FROM temp_trade_show_user_corenet_lz WHERE left(用户号码,1) = 1 and left(用户号码,2)>12 and 结束时间 >= date_format({}, '%Y-%m-%d')
AND 结束时间 < date_format({}, '%Y-%m-%d');""".format(start_date,end_date)
replace_sql_xsj = """REPLACE INTO temp_trade_show_user
SELECT
DISTINCT(用户号码) user_number,
LEFT(用户号码,7) provience,
终端类型 equipment,
imei,
接入网类型 network,
DATE_FORMAT(结束时间,'%Y-%m-%d') DATETIME,
DATE_FORMAT(结束时间,'%H') SHOUR,
'新世界会展中心' trade_show
FROM temp_trade_show_user_corenet_xsj WHERE  left(用户号码,1) = 1 and left(用户号码,2)>12 and 结束时间 >= date_format({}, '%Y-%m-%d')
AND 结束时间 < date_format({}, '%Y-%m-%d');""".format(start_date,end_date)
replace_sql_gz = """REPLACE INTO temp_trade_show_user
SELECT
DISTINCT(用户号码) user_number,
LEFT(用户号码,7) provience,
终端类型 equipment,
imei,
接入网类型 network,
DATE_FORMAT(结束时间,'%Y-%m-%d') DATETIME,
DATE_FORMAT(结束时间,'%H') SHOUR,
'国际会展中心' trade_show
FROM temp_trade_show_user_corenet_gz WHERE  left(用户号码,1) = 1 and left(用户号码,2)>12 and 结束时间 >= date_format({}, '%Y-%m-%d')
AND 结束时间 < date_format({}, '%Y-%m-%d');""".format(start_date,end_date)
replace_sql_do = """REPLACE INTO temp_trade_show_user
SELECT
DISTINCT(CASE WHEN LEFT(用户号码,2) = '86' THEN MID(用户号码,3,11) ELSE 用户号码 END) user_number,
MID(用户号码,3,7) provience,
终端类型 equipment,
imei,
接入网类型 network,
DATE_FORMAT(DATETIME,'%Y-%m-%d') DATETIME,
DATE_FORMAT(DATETIME,'%H') SHOUR,
会展 trade_show
FROM temp_trade_show_user_do WHERE  left(用户号码,3) = 861 and mid(用户号码,3,2)>12 and datetime >= date_format({}, '%Y-%m-%d')
 and datetime < date_format({}, '%Y-%m-%d') ORDER BY 用户号码;""".format(start_date,end_date)
del_sql_lz = """DELETE FROM temp_trade_show_user
WHERE user_number IN (SELECT 用户号码 FROM db_static_user where trade_show = '工业展览馆') and
trade_show = '工业展览馆';"""
del_sql_xsj = """DELETE FROM temp_trade_show_user
WHERE user_number IN (SELECT 用户号码 FROM db_static_user where trade_show = '新世界会展中心') and
trade_show = '新世界会展中心';"""
del_sql_gz = """DELETE FROM temp_trade_show_user
WHERE user_number IN (SELECT 用户号码 FROM db_static_user where trade_show = '国际会展中心') and
trade_show = '国际会展中心';"""
del_sql_wrong = """delete from temp_trade_show_user where left(user_number,1) <> 1 or left(user_number,2) < 12 or left(user_number,3) =145"""

try:
    # 执行sql语句
    cursor.execute(replace_sql_lz)
    # 提交到数据库执行
    client.commit()
    print('插入lz核心网数据进入user库')
    cursor.execute(replace_sql_gz)
    # 提交到数据库执行
    client.commit()
    print('插入gz核心网数据进入user库')
    cursor.execute(replace_sql_xsj)
    # 提交到数据库执行
    client.commit()
    print('插入xsj核心网数据进入user库')
    cursor.execute(replace_sql_do)
    # 提交到数据库执行
    client.commit()
    print('插入DO数据进入user库')
    cursor.execute(del_sql_lz)
    # 提交到数据库执行
    client.commit()
    print('删除user库中lz静态用户')
    cursor.execute(del_sql_gz)
    # 提交到数据库执行
    client.commit()
    print('删除user库中gz静态用户')
    cursor.execute(del_sql_xsj)
    # 提交到数据库执行
    client.commit()
    print('删除user库中xsj静态用户')
    cursor.execute(del_sql_wrong)
    # 提交到数据库执行
    client.commit()
    print('删除不规范电话号码')
    # 执行sql语句
    cursor.execute(select_sql)
    # 提交到数据库执行
    client.commit()
    # 串接URL
    # http://the-x.cn/#imsi=460000002866763
    # http://www.ip138.com:8080/search.asp?mobile=1300240&action=mobile
    # url = 'https://www.chahaoba.com/'
    n = 0
    #串接出URL
    for row in cursor.fetchall():        
#         turl = url+'?mobile='+row[0]+'&action=mobile'
        turl = url + row[0]
        try:
            # requests 方法获得URL信息
            r = requests.get(turl, headers=headers )
            r.raise_for_status()
            r.encoding = r.apparent_encoding
            # bs4方法查找
            soup = bs4.BeautifulSoup(r.text,'lxml')
#             html = soup.find_all("tr", attrs={"class": "tdc"})
            html = soup.find("meta", attrs={"name": "keywords"})
#             info = []
#             for item in html:
#                 td = item.find_all('td')
#                 info.append(td[1].text)
            # 正则表达式获得信息
#             str1 = re.match(r'(\d{7})', info[1])
#             str2 = re.split(r'\xa0', info[2])
            str0 = html.attrs['content'].split(',')[0] # 号段
            str1 = html.attrs['content'].split(',')[2] # 省会
            str2 = html.attrs['content'].split(',')[3] # 城市
        
            #插入号段数据库
#             insert_sql = "replace into db_province_city(prov,province,city) values('%s','%s','%s') " %(str1.group(0),str2[0],str2[1])
            insert_sql = "replace into db_province_city(prov,province,city) values('%s','%s','%s') " %(str0,str1,str2)
            if str2== '':
                # 测试发现部分爬取成功的号段省份为‘’，在此进行删除
                try:
                    del_sql_error_prov_num = "delete from temp_trade_show_user where province = '%s'" % (row[0])
                    print('删除垃圾号段：',row[0])
                    cursor = client.cursor()
                    cursor.execute(del_sql_error_prov_num)
                    client.commit()
                except:
                    print('删除失败号段：',row[0])
            else:
                try:
                    print('插入号段：',row[0])
                    cursor = client.cursor()
                    cursor.execute(insert_sql)
                    client.commit()
                    n = n + 1
                except:
                    print('插入失败号段:',row[0])
        except:
            del_sql_undefine_num = "delete from temp_trade_show_user where province = '%s'" % (row[0])
            try:
                cursor = client.cursor()
                cursor.execute(del_sql_undefine_num)
                client.commit()
            except:
                print('删除失败号段:',row[0])
            print('爬取失败并删除号段:',row[0])
    # 执行sql语句
    cursor.execute(update_sql)
    # 提交到数据库执行
    client.commit()

except:
    # 如果发生错误则回滚
    client.rollback()
endtime = time.time()
print('爬取成功次数：',n)
print('累计用时：',(endtime - starttime),'s')



replace_sql_user = """replace into temp_trade_show_user_province_city
SELECT a.trade_show,a.datetime,a.user_number,b.province,b.city
FROM temp_trade_show_user a LEFT JOIN db_province_city b ON a.province = b.prov
WHERE a.datetime >= date_format({}, '%Y-%m-%d')
AND a.datetime < date_format({}, '%Y-%m-%d')""".format(start_date,end_date)

update_sql_brand = '''UPDATE temp_trade_show_user a,cfg_customer_group_tac b
SET a.equipment = b.brand
WHERE a.equipment = b.tac and a.datetime >= date_format({},'%Y-%m-%d')'''.format(start_date)
try:
    cursor.execute(replace_sql_user)
    # 提交到数据库执行
    client.commit()
    cursor.execute(update_sql_brand)
    client.commit()
    print ('user brand update sucess')
except:
    # 如果发生错误则回滚
    client.rollback()
client.close()
print('user库更新完毕')