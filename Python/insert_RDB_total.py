# -*- coding: utf-8 -*-
import time,datetime
import pymongo
import pyodbc


cnxn = pyodbc.connect('DSN=news_project')
cursor = cnxn.cursor()
'''
db = pymongo.MongoClient('mongodb://10.120.28.202:28017').news
db1 = pymongo.MongoClient('mongodb://10.120.28.202:27017').ettoday
db2 = pymongo.MongoClient('mongodb://10.120.28.202:27017').ettoday1
db_ltn = pymongo.MongoClient('mongodb://10.120.28.202:27017').ltn
db_ltn8m = pymongo.MongoClient('mongodb://10.120.28.202:27017').ltn8m
appledaily_news = db.appledaily
nownews_news = db.nownews
chinatimes_news = db.chinatimes
cna_news = db.cna
ettoday_news = db1.ettoday
ettoday_news1 = db2.ettoday
ltn_news = db_ltn.ltn
db_ltn8m = db_ltn8m.ltn
'''
db_Jan2015=pymongo.MongoClient('mongodb://10.120.28.202:27017').Jan2015
db_ltn = pymongo.MongoClient('mongodb://10.120.28.202:28017').ltn
appledaily_news = db_Jan2015.appledaily
nownews_news = db_Jan2015.nownews
chinatimes_news = db_Jan2015.chinatimes
cna_news = db_Jan2015.cna
ettoday_news = db_Jan2015.ettoday
ltn_news = db_ltn.ltn
'''
[0]=insert_people
[1]=reporter
[2]=title
[3]=date
[4]=link
[5]=source
[6]=insert_time
[7]=keywords
[8]=_id
[9]=photo_link
[10]=con
appledaily & nownews [3]=date [7]=keywords
chinatimes & cna [7]=date [3]=keywords
'''

def get_2014m_appledaily_total():
    todaytime = datetime.datetime(2015,1,18)
    duetime = datetime.datetime(2015,1,16)
    howtime = (todaytime-duetime).days
    oneday = datetime.timedelta(1)
    cursor.execute("SELECT TOP 1 * FROM newscontent ORDER BY newsno DESC;")
    rows = cursor.fetchall()
    for i in rows:
         newsno = i.newsno
    print newsno
    #newsno = 1000
    for time in range(1,howtime+1):
        timeYmd = todaytime.strftime('%Y/%m/%d')
        todaytime = todaytime - oneday
        for data_date in appledaily_news.find({'date':timeYmd}):
            newsno = newsno + 1
            title =  data_date.values()[2]
            date =  data_date.values()[3]
            content =  data_date.values()[10]
            if len(data_date.values()[9]) !=0:
                photo_link =  'newsphoto/photo_appledaily/'+data_date.values()[8]+'/'+data_date.values()[8]+'.png'
            else:
                photo_link =  'newsphoto/nophoto/nopic.png'
            source =  data_date.values()[5]
            cursor.execute("insert into tmp_newscontent values(?,?,?,?,?,?)", newsno, title,date,content,photo_link,source)
        cnxn.commit()
def get_2014m_nownews_total():
    todaytime = datetime.datetime(2015,1,18)
    duetime = datetime.datetime(2015,1,16)
    howtime = (todaytime-duetime).days
    oneday = datetime.timedelta(1)
    cursor.execute("SELECT TOP 1 * FROM tmp_newscontent ORDER BY newsno DESC;")
    rows = cursor.fetchall()
    for i in rows:
        newsno = i.newsno
    print newsno
    #newsno = 1000
    for time in range(1,howtime+1):
        timeYmd = todaytime.strftime('%Y/%m/%d')
        todaytime = todaytime - oneday
        for data_date in nownews_news.find({'date':timeYmd}):
            newsno = newsno + 1
            title =  data_date.values()[2]
            date =  data_date.values()[3]
            content =  data_date.values()[10]
            if len(data_date.values()[9]) !=0:
                photo_link =  'newsphoto/photo_nownews/'+data_date.values()[8]+'/'+data_date.values()[8]+'.png'
            else:
                photo_link =  'newsphoto/nophoto/nopic.png'
            source =  data_date.values()[5]
            cursor.execute("insert into tmp_newscontent values(?,?,?,?,?,?)", newsno, title,date,content,photo_link,source)
        cnxn.commit()
def get_2014m_chinatimes_total():
    todaytime = datetime.datetime(2015,1,18)
    duetime = datetime.datetime(2015,1,16)
    howtime = (todaytime-duetime).days
    oneday = datetime.timedelta(1)
    cursor.execute("SELECT TOP 1 * FROM tmp_newscontent ORDER BY newsno DESC;")
    rows = cursor.fetchall()
    for i in rows:
        newsno = i.newsno
    print newsno
    #newsno = 1000
    for time in range(1,howtime+1):
        timeYmd = todaytime.strftime('%Y/%m/%d')
        todaytime = todaytime - oneday
        for data_date in chinatimes_news.find({'date':timeYmd}):
            newsno = newsno + 1
            title =  data_date.values()[2]
            date =  data_date.values()[7]
            content =  data_date.values()[10]
            if len(data_date.values()[9]) !=0:
                photo_link =  'newsphoto/photo_chinatimes/'+data_date.values()[8]+'/'+data_date.values()[8]+'.png'
            else:
                photo_link =  'newsphoto/nophoto/nopic.png'
            source =  data_date.values()[5]
            cursor.execute("insert into tmp_newscontent values(?,?,?,?,?,?)", newsno, title,date,content,photo_link,source)
        cnxn.commit()
def get_2014m_cna_total():
    todaytime = datetime.datetime(2015,1,18)
    duetime = datetime.datetime(2015,1,16)
    howtime = (todaytime-duetime).days
    oneday = datetime.timedelta(1)
    cursor.execute("SELECT TOP 1 * FROM tmp_newscontent ORDER BY newsno DESC;")
    rows = cursor.fetchall()
    for i in rows:
        newsno = i.newsno
    print newsno
    for time in range(1,howtime+1):
        timeYmd = todaytime.strftime('%Y/%m/%d')
        todaytime = todaytime - oneday
        for data_date in cna_news.find({'date':timeYmd}):
            newsno = newsno + 1
            title =  data_date.values()[2]
            date =  data_date.values()[7]
            content =  data_date.values()[10].strip().replace('\r','').replace('\n','')#.replace(' ','').replace('　','')
            if len(data_date.values()[9]) !=0:
                photo_link =  'newsphoto/photo_cna/'+data_date.values()[8]+'/'+data_date.values()[8]+'.png'
            else:
                photo_link =  'newsphoto/nophoto/nopic.png'
            source =  data_date.values()[5]
            cursor.execute("insert into tmp_newscontent values(?,?,?,?,?,?)", newsno, title,date,content,photo_link,source)
        cnxn.commit()
def get_2014m_ettoday_total():
    todaytime = datetime.datetime(2015,1,18)
    duetime = datetime.datetime(2015,1,16)
    howtime = (todaytime-duetime).days
    oneday = datetime.timedelta(1)
    cursor.execute("SELECT TOP 1 * FROM tmp_newscontent ORDER BY newsno DESC;")
    rows = cursor.fetchall()
    for i in rows:
        newsno = i.newsno
    print newsno
    for time in range(1,howtime+1):
        timeYmd = todaytime.strftime('%Y/%m/%d')
        todaytime = todaytime - oneday
        for data_date in ettoday_news.find({'date':timeYmd}):
            newsno = newsno + 1
            title =  data_date.values()[2]
            date =  data_date.values()[7]
            content =  data_date.values()[10]
            if len(data_date.values()[9]) !=0:
                photo_link =  'newsphoto/photo_ettoday/'+data_date.values()[8]+'/'+data_date.values()[8]+'.png'
            else:
                photo_link =  'newsphoto/nophoto/nopic.png'
            source =  data_date.values()[5]
            cursor.execute("insert into tmp_newscontent values(?,?,?,?,?,?)", newsno, title,date,content,photo_link,source)
        cnxn.commit()
def get_2014m_ettoday1_total():
    todaytime = datetime.datetime(2014,12,31)
    duetime = datetime.datetime(2013,12,31)
    howtime = (todaytime-duetime).days
    oneday = datetime.timedelta(1)
    cursor.execute("SELECT TOP 1 * FROM tmp_newscontent ORDER BY newsno DESC;")
    rows = cursor.fetchall()
    for i in rows:
        newsno = i.newsno
    print newsno
    for time in range(1,howtime+1):
        timeYmd = todaytime.strftime('%Y/%m/%d')
        todaytime = todaytime - oneday
        for data_date in ettoday_news.find({'date':timeYmd}):
            newsno = newsno + 1
            title =  data_date.values()[2]
            date =  data_date.values()[7]
            content =  data_date.values()[10]
            if len(data_date.values()[9]) !=0:
                photo_link =  'newsphoto/photo_ettoday/'+data_date.values()[8]+'/'+data_date.values()[8]+'.png'
            else:
                photo_link =  'newsphoto/nophoto/nopic.png'
            source =  data_date.values()[5]
            #print source
            cursor.execute("insert into tmp_newscontent values(?,?,?,?,?,?)", newsno, title,date,content,photo_link,source)
        cnxn.commit()
def get_2014m_ltn_total():
    todaytime = datetime.datetime(2015,1,18)
    duetime = datetime.datetime(2015,1,16)
    howtime = (todaytime-duetime).days
    oneday = datetime.timedelta(1)
    cursor.execute("SELECT TOP 1 * FROM tmp_newscontent ORDER BY newsno DESC;")
    rows = cursor.fetchall()
    for i in rows:
        newsno = i.newsno
    print newsno
    for time in range(1,howtime+1):
        timeYmd = todaytime.strftime('%Y/%m/%d')
        todaytime = todaytime - oneday
        for data_date in ltn_news.find({'date':timeYmd}):
            newsno = newsno + 1
            title =  data_date.values()[2].strip().replace('\r','').replace('\n','')
            date =  data_date.values()[7]
            content =  data_date.values()[10].strip().replace('\r','').replace('\n','')
            if len(data_date.values()[9]) !=0:
                photo_link =  'newsphoto/photo_ltn/'+data_date.values()[8]+'/'+data_date.values()[8]+'.png'
            else:
                photo_link =  'newsphoto/nophoto/nopic.png'
            source =  data_date.values()[5]
            cursor.execute("insert into tmp_newscontent values(?,?,?,?,?,?)", newsno, title,date,content,photo_link,source)
        cnxn.commit()
def get_2014m_ltn8m_total():
    todaytime = datetime.datetime(2014,8,31)
    duetime = datetime.datetime(2013,12,31)
    howtime = (todaytime-duetime).days
    oneday = datetime.timedelta(1)
    cursor.execute("SELECT TOP 1 * FROM tmp_newscontent ORDER BY newsno DESC;")
    rows = cursor.fetchall()
    for i in rows:
        newsno = i.newsno
    print newsno
    for time in range(1,howtime+1):
        timeYmd = todaytime.strftime('%Y/%m/%d')
        todaytime = todaytime - oneday
        for data_date in ltn_news.find({'date':timeYmd}):
            newsno = newsno + 1
            title =  data_date.values()[2].strip().replace('\r','').replace('\n','')
            date =  data_date.values()[7]
            content =  data_date.values()[10].strip().replace('\r','').replace('\n','')
            if len(data_date.values()[9]) !=0:
                photo_link =  'newsphoto/photo_ltn/'+data_date.values()[8]+'/'+data_date.values()[8]+'.png'
            else:
                photo_link =  'newsphoto/nophoto/nopic.png'
            source =  data_date.values()[5]
            cursor.execute("insert into tmp_newscontent values(?,?,?,?,?,?)", newsno, title,date,content,photo_link,source)
        cnxn.commit()
t1 = time.time()
print t1
get_2014m_appledaily_total()
get_2014m_nownews_total()
get_2014m_chinatimes_total()
get_2014m_cna_total()
get_2014m_ettoday_total()
get_2014m_ltn_total()
#get_2014m_ettoday1_total()
#get_2014m_ltn8m_total()
t2 = time.time()
print t2-t1
