#!/usr/bin/env python
# -*- coding:utf-8 -*-

# 使用了线程库
import threading
# 队列
from Queue import Queue
# 解析库
from lxml import etree
# 请求处理
import requests
# 正则
import re
# 地名
# import places_name
# 日志
import logging
# 随机
import random
# 时间
import time
# 数据库
import pymysql

import sys
reload(sys)
sys.setdefaultencoding( "utf-8" )
# keyword和url

def get_start_url():
    '''
    获取对应地名的职位搜索url
    :return:
    '''
    place_name = ['深圳']
    job_name = ['iOS']
    total_urls = []
    for keyword in job_name:
        list_urls = []
        for i in place_name:
            url = 'http://sou.zhaopin.com/jobs/searchresult.ashx?jl=' + str(i) + '&kw=' + keyword
            list_urls.append(url)
        total_urls.append((keyword, list_urls))


    return total_urls

urls=get_start_url()

jobUrl_list = []
TABLE_EXISTS = 0
USER_AGENTS = [
   "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; AcooBrowser; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
   "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0; Acoo Browser; SLCC1; .NET CLR 2.0.50727; Media Center PC 5.0; .NET CLR 3.0.04506)",
   "Mozilla/4.0 (compatible; MSIE 7.0; AOL 9.5; AOLBuild 4337.35; Windows NT 5.1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
   "Mozilla/5.0 (Windows; U; MSIE 9.0; Windows NT 9.0; en-US)",
   "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 2.0.50727; Media Center PC 6.0)",
   "Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 1.0.3705; .NET CLR 1.1.4322)",
   "Mozilla/4.0 (compatible; MSIE 7.0b; Windows NT 5.2; .NET CLR 1.1.4322; .NET CLR 2.0.50727; InfoPath.2; .NET CLR 3.0.04506.30)",
   "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN) AppleWebKit/523.15 (KHTML, like Gecko, Safari/419.3) Arora/0.3 (Change: 287 c9dfb30)",
   "Mozilla/5.0 (X11; U; Linux; en-US) AppleWebKit/527+ (KHTML, like Gecko, Safari/419.3) Arora/0.6",
   "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.2pre) Gecko/20070215 K-Ninja/2.1.1",
   "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN; rv:1.9) Gecko/20080705 Firefox/3.0 Kapiko/3.0",
   "Mozilla/5.0 (X11; Linux i686; U;) Gecko/20070322 Kazehakase/0.4.5",
   "Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.8) Gecko Fedora/1.9.0.8-1.fc10 Kazehakase/0.5.6",
   "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11",
   "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/535.20 (KHTML, like Gecko) Chrome/19.0.1036.7 Safari/535.20",
   "Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; fr) Presto/2.9.168 Version/11.52",
]
header={'User-Agent': random.choice(USER_AGENTS)}
# 保存日志方便查看
logging.basicConfig(filename='logging.log',
                    format='%(asctime)s %(message)s',
                    filemode="w", level=logging.DEBUG)


def Connect():
    '''链接本地数据库，数据库名为zhiliansearch'''
    host = "localhost"
    dbName = "zhiliansearch"
    user = "root"
    # port = "3306"
    password = "008"
    db = pymysql.connect(host, user, password, dbName, charset='utf8')
    return db
    cursorDB = db.cursor()
    return cursorDB

def CreateTable():
    '''创建数据表zhiliansearch'''
    db = Connect()
    cur = db.cursor()
    create_sql = "create table if not exists zhiliansearch(id int(11) not null auto_increment,职位名称 varchar(255) default null,公司名称 varchar(255) default null,公司链接 varchar(255) defau" \
        "lt null,公司福利 varchar(255) default null,职位月薪 varchar(255) default null,工作地点 varchar(255) default null,发布日期 varchar(255) defa" \
            "ult null,工作性质 varchar(255) default null,工作经验 varchar(255) default null,最低学历 varchar(255) default null,招聘人数 varchar(255) default null,职位类别 varchar(255) default null,职位描述 TEXT,详细工作地点 varc" \
                "har(255) default null,公司介绍 text,公司规模 varchar(255) default null,公司性质 varchar(255) default null,公司行业 varchar(255) defa" \
                    "ult null,公司主页 varchar(255) default null,公司地址 varchar(255) default null,primary key(id))  "
    cur.execute(create_sql)
    db.commit()
    db.close()
    cur.close()
    p_rint1='数据表创建成功'
    print '数据表创建成功'
    logging.info(p_rint1)



class ThreadCrawl(threading.Thread):
    def __init__(self, threadName, pageQueue, dataQueue):
        #threading.Thread.__init__(self)
        # 调用父类初始化方法
        super(ThreadCrawl, self).__init__()
        # 线程名
        self.threadName = threadName
        # 页码队列
        self.pageQueue = pageQueue
        # 数据队列
        self.dataQueue = dataQueue
        # 请求报头
        self.headers = header

    def run(self):
        print "启动 " + self.threadName
        while not CRAWL_EXIT:
            try:
                # 取出一个数字，先进先出
                # 可选参数block，默认值为True
                #1. 如果对列为空，block为True的话，不会结束，会进入阻塞状态，直到队列有新的数据
                #2. 如果队列为空，block为False的话，就弹出一个Queue.empty()异常，
                jobUrl = self.pageQueue.get(False)
                p=random.randint(1,3)
                time.sleep(1)
                try:
                    content = requests.get(jobUrl, headers = self.headers,timeout=16,allow_redirects=False).text
                except Exception,e3:
                    print e3
                self.dataQueue.put(content)
            except:
                pass
        print "结束 " + self.threadName



class ThreadParse(threading.Thread):
    def __init__(self, threadName, dataQueue, lock):
        super(ThreadParse, self).__init__()
        # 线程名
        self.threadName = threadName
        # 数据队列
        self.dataQueue = dataQueue
        # 锁
        self.lock = lock

        self.count = 0

    def run(self):
        print "启动" + self.threadName
        while not PARSE_EXIT:
            try:
                content = self.dataQueue.get(False)
                self.parse(content)
            except:
                pass
        print "退出" + self.threadName

    def parse(self, content):
        # 解析为HTML DOM
        response = etree.HTML(content)
        for i in response.xpath('//div[@class="inner-left fl"]'):
            job_name = i.xpath('h1/text()')  # 职位名称
            company_name = i.xpath('h2/a/text()')  # 公司名称
            company_link = i.xpath('h2/a/@href')  # 公司链接
            advantage = ','.join(i.xpath('div[1]/span/text()'))  # 公司福利
        for i in response.xpath('//ul[@class="terminal-ul clearfix"]'):
            salary = i.xpath('li[1]/strong/text()')  # 职位月薪
            place = i.xpath('li[2]/strong/a/text()') # 工作地点
            post_time = i.xpath('li[3]//span[@id="span4freshdate"]/text()')  # 发布日期
            job_nature = i.xpath('li[4]/strong/text()')  # 工作性质
            work_experience = i.xpath('li[5]/strong/text()') # 工作经验
            education = i.xpath('li[6]/strong/text()')  # 最低学历
            job_number = i.xpath('li[7]/strong/text()')  # 招聘人数
            job_kind = i.xpath('li[8]/strong/a/text()')  # 职位类别

        html_body=content
        reg = r'<!-- SWSStringCutStart -->(.*?)<!-- SWSStringCutEnd -->'
        reg = re.compile(reg, re.S)
        content = re.findall(reg, html_body)
        try:
            content = content[0].strip()  # strip去空白
            reg_1 = re.compile(r'<[^>]+>')  # 去除html标签
            content = reg_1.sub('', content).replace('&nbsp', '')
            job_content= content  # 职位描述
        except Exception,e:
            job_content=''
        
        for i in response.xpath('//div[@class="tab-inner-cont"]')[0:1]:
            job_place = i.xpath('h2/text()')[0].strip()    #工作地点（具体）
        
        for i in response.xpath('//div[@class="tab-inner-cont"]')[1:2]:
            reg = re.compile(r'<[^>]+>')
            company_content = reg.sub('',i.xpath('string(.)')).replace('&nbsp', '')  # 公司的介绍
            company_info = company_content
        
        for i in response.xpath('//ul[@class="terminal-ul clearfix terminal-company mt20"]'):
            if u'公司主页' in i.xpath('string(.)'):
                company_size = i.xpath('li[1]/strong/text()')
                company_nature =i.xpath('li[2]/strong/text()')
                company_industry = i.xpath('li[3]/strong/a/text()')
                company_home_link = i.xpath('li[4]/strong/a/text()')
                company_place = i.xpath('li[5]/strong/text()')
            else:
                company_size = i.xpath('li[1]/strong/text()')
                company_nature = i.xpath('li[2]/strong/text()')
                company_industry = i.xpath('li[3]/strong/a/text()')
                company_home_link = [u'无公司主页']
                company_place = i.xpath('li[4]/strong/text()')

        item_list = [str(job_name[0]),
                     str(company_name[0]),
                     str(company_link[0]),
                     str(advantage),
                     str(salary[0]),
                     str(place[0]),
                     str(post_time[0]),
                     str(job_nature[0]),
                     str(work_experience[0]),
                     str(education[0]),
                     str(job_number[0]),
                     str(job_kind[0]),
                     str(job_content),
                     str(job_place),
                     str(company_info),
                     str(company_size[0]),
                     str(company_nature[0]),
                     str(company_industry[0]),
                     str(company_home_link[0]),
                     str(company_place[0].strip()),
                     ]
        
        # 打开锁、处理内容、释放锁

        with self.lock:

            #链接数据库
            db = Connect()
            cur = db.cursor()

            insert_sql = 'insert into zhiliansearch(职位名称,公司名称,公司链接,公司福利,职位月薪,工作地点,发布日期,工作性' \
               '质,工作经验,最低学历,招聘人数,职位类别,职位描述,详细工作地点,公司介绍,公司规模,公司性质,公司行业,公司主页,公司地址)value(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'

            cur.execute(insert_sql, item_list)
            db.commit()
            db.close()
            cur.close()
            self.count  += 1
            print '成功插入' + str(self.count) + ' 条数据'
            p_rint8 = '成功插入' + str(self.count) + ' 条数据'
            logging.info(p_rint8)

def get_job_url(url):
    '''得到每个职位的链接地址'''
    html=requests.get(url,headers=header,timeout=10)
    response=etree.HTML(html.content)
    job_urls=response.xpath('//*[@id="newlist_list_content_table"]/table/tr[1]/td[1]/div/a/@href')
    '''一页上的信息'''
    for job_url in job_urls:
        '''遍历职位链接，准备抓取信息字段'''
        try:
            '''准确地址'''
            jobUrl_list.append(job_url)
        except Exception,e:
            p_rint3= e,'--------get_content'
            print e,'--------get_content'
            logging.info(p_rint3)
            continue

def get_page(url):
    '''得到每个区的页数'''
    html = requests.get(url,headers=header,timeout=10)
    html=html.content
    reg = r'共<em>(.*?)</em>个职位满足条件'
    reg = re.compile(reg)
    job_count = int(re.findall(reg, html)[0])
    print 'job_count %d'%(job_count)
    # 搜索结果60条显示一页 总共页数
    job_count_page = job_count / 60 + 1
    if job_count == 0:
        return job_count
    else:
        if job_count_page <= 1:
            p_rint4='一共' + str(job_count) + '个职位,' + str(job_count_page) + ' 页'
            print '一共' + str(job_count) + '个职位,' + str(job_count_page) + ' 页'
            logging.info(p_rint4)
            for i in range(1, job_count_page + 1):
                '''循环每个区的页数，准备抓取职位链接地址 每页url'''
                page = url + '&p=' + str(i)
                print page
                try:
                    get_job_url(page)
                except Exception,e:
                    p_rint5=e,'--------get_job_url'
                    print e,'--------get_job_url'
                    logging.info(p_rint5)
                    continue
        else:
            p_rint6='一共' + str(job_count) + '个职位,' + str(job_count_page) + ' 页'
            print '一共' + str(job_count) + '个职位,' + str(job_count_page) + ' 页'
            logging.info(p_rint6)
            for i in range(1, job_count_page + 1):
                '''循环每个区的页数，准备抓取职位链接地址'''
                page = url + '&p=' + str(i)
                print page
                try:
                    get_job_url(page)
                except Exception,e:
                    p_rint7=e,'--------get_job_url'
                    print e,'--------get_job_url'
                    logging.info(p_rint7)
                    continue
        return job_count


CRAWL_EXIT = False
PARSE_EXIT = False

global keyword
def main():
	# 创建数据表
    CreateTable()
    count = 0
    # 获取初始url
    page_count = 0
    for tp in urls:
        keyword = tp[0]
        start_urls = tp[1]
        for start_url in start_urls:
            try:
                page_count = get_page(start_url)
            except Exception,e:
                p_rint9=e,'--------get_page'
                print e,'--------get_page'
                logging.info(p_rint9)
                continue
    if page_count == 0:
        print '没有相关职位招聘信息'
        return 0
    print '获取%d个相关职位'%(page_count)


    pageQueue = Queue(page_count)
    for jobUrl in jobUrl_list:
        print jobUrl
        pageQueue.put(jobUrl)
    # 采集结果(每页的HTML源码)的数据队列，参数为空表示不限制
    dataQueue = Queue()
    # filename = open("search.json","a")
    # 创建锁
    lock = threading.Lock()

    # 三个采集线程的名字
    crawlList = ["采集线程1号", "采集线程2号", "采集线程3号"]
    # 存储三个采集线程的列表集合
    threadcrawl = []
    for threadName in crawlList:
        thread = ThreadCrawl(threadName, pageQueue, dataQueue)
        thread.start()
        threadcrawl.append(thread)


    # 三个解析线程的名字
    parseList = ["解析线程1号","解析线程2号","解析线程3号"]
    # 存储三个解析线程
    threadparse = []
    for threadName in parseList:
        thread = ThreadParse(threadName, dataQueue, lock)
        thread.start()
        threadparse.append(thread)

    # 等待pageQueue队列为空，也就是等待之前的操作执行完毕
    while not pageQueue.empty():
        pass

    # 如果pageQueue为空，采集线程退出循环
    global CRAWL_EXIT
    CRAWL_EXIT = True

    print "pageQueue为空"

    for thread in threadcrawl:
        thread.join()
        print "1"

    while not dataQueue.empty():
        pass

    global PARSE_EXIT
    PARSE_EXIT = True

    for thread in threadparse:
        thread.join()
        print "2"

    print "谢谢使用！"

if __name__ == "__main__":
    main()

