# -*- coding: utf-8 -*- 
import urlparse, requests, os, re, time, datetime, shutil
from bs4 import BeautifulSoup
import pymongo

todaytime = datetime.datetime(2015,1,18) #開始時間
duetime = datetime.datetime(2015,1,17) #結束時間
howtime = (todaytime-duetime).days
oneday = datetime.timedelta(1)#設定1天

def get_news_linklist(index_link):
        appledailyUrl = index_link+timeYmd
        #print appledailyUrl
        get_news_link(appledailyUrl)

def get_news_link(appledailyUrl):
    while True:
        try:
            getappledaily = requests.get(appledailyUrl) #http://www.appledaily.com.tw/appledaily/archive/20141130
            sourcePage = getappledaily.text.encode('utf8')
            soup = BeautifulSoup(sourcePage, 'lxml')
            newsType = soup.select('.fillup')[2:-3]
            for newsType1 in newsType:
                for urlsType in newsType1.select('li'): 
                    link = [tag['href']for tag in urlsType.select('a')] [0]
                    link = "".join(link.split())
                    news_link = urlparse.urljoin("http://www.appledaily.com.tw", link)
                    #print news_link
                    get_news_contents(news_link)
            def sleep( mytime=1 ):
                time.sleep( mytime )
            break
        except requests.ConnectionError:
            continue

def get_news_contents(news_link):
    errorcount = 0
    while True:
        try:
            result = requests.get(news_link)
            response = result.text.encode('utf8')
            soup = BeautifulSoup(response) 
            news_id = "appledaily"+timeY+timem+timed+''.join(news_link.split(timeY+timem+timed+'/')[1].split('/')[0])
            print news_id
            title = news_link.split("/")[-1].encode('utf8')
            #print title
            date = timeY+"/"+timem+"/"+timed
            #print date
            link = news_link
            #print link
            if len(soup.select('.slimg')) == 1:
                photo = soup.find('div',{'class':'mediabox'}).findAll('img')
                photo_link = photo[0]['src']
                get_photo(photo_link,news_id)
            else:
                photo_link = ""
            #print photo_link
            reporter = ""
            #print reporter
            insert_time = datetime.date.today().strftime('%Y/%m/%d')
            #print insert_time
            keywords = ""
            #print keywords
            def getcontent():
                get_contents=''
                content = soup.find('div',{'class':'articulum'}).findAll('p')
                #print content
                for con in content:
                    #print con
                    get_contents+=con.text.encode('utf8').strip().replace('\n','').replace('\r','').replace('\t','').replace('　',' ')
                #get_content = re.sub(r'【\S+】', '', get_contents)
                #print get_content
                return get_contents
            con = getcontent()
            #print con
            data_insert(news_id,title,date,link,photo_link,reporter,insert_time,keywords,con)
            def sleep( mytime=1 ):
                time.sleep( mytime )
            break
        except BaseException,e:
            print e
            if errorcount > 20 :
                f.write(news_link+'\n')
                print "重複超過次數"
                break
            else:
                errorcount += 1
            continue
            
def get_photo(photo_link,news_id):
    rs = requests.Session()
    resopnse = rs.get(photo_link,stream=True)
    if not os.path.exists("E://photo_appledaily_0118/"+news_id):
        os.makedirs("E://photo_appledaily_0118/"+news_id)
    with open("E://photo_appledaily_0118/"+news_id+"/"+news_id+".png",'wb') as out_file:
        shutil.copyfileobj(resopnse.raw,out_file)
    del resopnse

def data_insert(news_id,title,date,link,photo_link,reporter,insert_time,keywords,con):
    db = pymongo.MongoClient('mongodb://10.120.28.202:27017').Jan2015
    appledaily_news = db.appledaily
    if news_id not in dataid_dic: 
        data = {"_id":news_id,
                "title":title,
                "date":date,
                "source":"appledaily",
                "link":link,
                "photo_link":photo_link,
                "reporter":reporter,
                "insert_time":insert_time,
                "insert_people":"吳佳勳",
                "keywords":keywords,
                "con":con}
        dataid_dic[news_id]=1
        appledaily_news.insert(data)
if not os.path.exists('E://Dropbox/Python27/errorlinks_appledaily/'):
	os.makedirs('E://Dropbox/Python27/errorlinks_appledaily/')
for time in range(1,howtime+1):
    dataid_dic = {}
    timeYmd = todaytime.strftime('%Y%m%d') #現在時間轉Ymd格式
    timeY = todaytime.strftime('%Y') #現在時間轉Ymd格式
    timem = todaytime.strftime('%m') #現在時間轉Ymd格式
    timed = todaytime.strftime('%d') #現在時間轉Ymd格式
    todaytime = todaytime - oneday #減一天時間
    f = open('E://Dropbox/Python27/errorlinks_appledaily/error'+timeYmd+'.txt','w')
    get_news_linklist('http://www.appledaily.com.tw/appledaily/archive/')
    f.close()