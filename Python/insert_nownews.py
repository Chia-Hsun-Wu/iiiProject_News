# -*- coding: utf-8 -*-
import urlparse, requests, os, re, time, datetime, shutil
from bs4 import BeautifulSoup
import pymongo

todaytime = datetime.datetime(2015,1,18) #開始時間
duetime = datetime.datetime(2015,1,17) #結束時間
howtime = (todaytime-duetime).days
oneday = datetime.timedelta(1)#設定1天

def get_news_linklist(index_link):
        nownewsUrl = index_link+timeY+"/"+timem+"/"+timed+"/p%d"
        #print nownewsUrl
        get_news_link(nownewsUrl)

def get_news_link(nownewsUrl):
    start = 1
    while True:
        try:
            nownewsdatepage = nownewsUrl%(start)
            #print nownewsdatepage
            getnownews = requests.get(nownewsdatepage) #http://www.nownews.com/n/2014/12/02/p1
            sourcePage = getnownews.text.encode('utf8')
            soup = BeautifulSoup(sourcePage)
            get_links = soup.find('ul',{'id':'result-list'}).findAll('h2')
            #print get_links
            if len(get_links)>0:
                for news_links in get_links:
                    news_link = news_links.a['href']
                    get_news_contents(news_link)
            else:
                break
            start = start+1
            def sleep( mytime=1 ):
                time.sleep( mytime )         
        except requests.ConnectionError:
            continue

def get_news_contents(news_link):
    errorcount = 0
    while True:
        try:
            result = requests.get(news_link)
            response = result.text.encode('utf8')
            soup = BeautifulSoup(response) 
            news_id = "nownews"+''.join(news_link.split('/')[-4:])
            print news_id
            title = soup.find('div',{'class':'news_story'}).find('h1').text.encode('utf8').replace('　','')
            #print title
            date = timeY+"/"+timem+"/"+timed
            #print date
            link = news_link
            #print link
            if soup.find('div',{'class':'autozoom'}) is not None:#.findAll('img')
                photo = soup.find('div',{'class':'autozoom'}).findAll('img')
                photo_link = photo[0]['src']
                get_photo(photo_link,news_id)
            else:
                photo_link = ""
            #print photo_link
            reporter = soup.find('div',{'class':'story_content'}).findAll('p')[0].text
            #print reporter
            insert_time = datetime.date.today().strftime('%Y/%m/%d')
            #print insert_time
            content_kw = soup.find('div',{'class':'story_content'}).findAll('p')[-1]
            keywords = content_kw.text.replace('\n',' ')
            #print keywords
            def getcontent():
                get_contents=''
                if  len(soup.find('div',{'class':'story_content'}).findAll('p')[1:-1]) > 0 :
                    if len(soup.find('div',{'class':'story_content'}).findAll('div')) == 2:
                        content = soup.find('div',{'class':'story_content'}).findAll('p')[1:-2] 
                        #print content
                        for con in content:
                            get_contents+=con.text.encode('utf8').replace('\n','').replace('\r','').replace('\t','').replace('　',' ')
                        #print cons
                        return get_contents
                    else:
                        content = soup.find('div',{'class':'story_content'}).findAll('p')[1:-1] 
                        #print content
                        for con in content:
                            get_contents+=con.text.encode('utf8').strip().replace('\n','').replace('\r','').replace('\t','').replace('　',' ')
                        #print cons
                        return get_contents
                else:
                    content = soup.find('div', {'class': 'story_content'}).contents
                    pattern = r'[</?\w+/?>]'
                    for con in content:
                        #print con
                        cons = str(con.encode('utf8'))
                        #print re.findall(pattern, line)
                        #print re.match(pattern, line)
                        if re.match(pattern, cons):
                            continue
                        else:
                            if len(cons) > 0:
                                #print cons.strip()
                                get_contents+=cons.strip().replace('\n','').replace('\r','').replace('\t','')
                    #print contents
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
    if not os.path.exists("E://photo_nownews_0118/"+news_id):
        os.makedirs("E://photo_nownews_0118/"+news_id)
    with open("E://photo_nownews_0118/"+news_id+"/"+news_id+".png",'wb') as out_file:
        shutil.copyfileobj(resopnse.raw,out_file)
    del resopnse

def data_insert(news_id,title,date,link,photo_link,reporter,insert_time,keywords,con):
    db = pymongo.MongoClient('mongodb://10.120.28.202:27017').Jan2015
    nownews_news = db.nownews
    if news_id not in dataid_dic:
        data = {"_id":news_id,
                "title":title,
                "date":date,
                "source":"NOWnews",
                "link":link,
                "photo_link":photo_link,
                "reporter":reporter,
                "insert_time":insert_time,
                "insert_people":"吳佳勳",
                "keywords":keywords,
                "con":con}
        dataid_dic[news_id]=1
        nownews_news.insert(data)
t1 = time.time()
print t1
for time in range(1,howtime+1):
    dataid_dic = {}
    timeYmd = todaytime.strftime('%Y%m%d') #現在時間轉Ymd格式
    timeY = todaytime.strftime('%Y') #現在時間轉Ymd格式
    timem = todaytime.strftime('%m') #現在時間轉Ymd格式
    timed = todaytime.strftime('%d') #現在時間轉Ymd格式
    todaytime = todaytime - oneday #減一天時間
    f = open('E://Dropbox/Python27/errorlinks_nownews/error'+timeYmd+'.txt','w')
    #f1 = open('ETL_nownews.txt','w')
    get_news_linklist('http://www.nownews.com/n/')
    #f1.close
    f.close()