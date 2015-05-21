# -*- coding: utf-8 -*-
import scrapy

from scrapy.spider import BaseSpider
from scrapy.selector import HtmlXPathSelector
from scrapy.http import Request,FormRequest
from scrapy.conf import settings
from scrapy import log
from scrapy.shell import inspect_response


# import leancloud
# from leancloud import Object
# from leancloud import LeanCloudError
# from leancloud import Query

from datetime import datetime
from zhQuesFollower import settings

from zhQuesFollower.items import ZhquesfollowerItem
import bmemcached
import re


class QuesfollowerSpider(scrapy.Spider):
    name = "quesFollower"
    allowed_domains = ["www.zhihu.com"]
    baseUrl = "http://www.zhihu.com/question/"
    start_urls = (
        'http://www.zhihu.com/',
    )
    questionIdSet = set()
    quesIndex =0
    handle_httpstatus_list = [401,429,500]

    def __init__(self,stats):
        self.stats = stats
        print "Initianizing ....."

        # leancloud.init(settings.APP_ID_S, master_key=settings.MASTER_KEY_S)

        # client_s = bmemcached.Client(settings.CACHE_SERVER_S,settings.CACHE_USER_S,settings.CACHE_PASSWORD_S)
        #
        # dbPrime = 97
        # totalCount = int(client_s.get('totalCount'))
        # for questionIndex in range(0,3000):
        #     self.questionIdSet.add(int(client_s.get(str(questionIndex))[0]))

        self.questionIdSet.add(19854911)
        self.questionIdSet.add(20769127)

        # for tableIndex in range(dbPrime):
        #     if tableIndex < 10:
        #         tableIndexStr = '0' + str(tableIndex)
        #     else:
        #         tableIndexStr = str(tableIndex)

            # Question = Object.extend('Question' + tableIndexStr)
            # query = Query(Question)
            # query.exists('questionId')
            #
            # # 避免在查询时，仍然有新的Question入库
            # # curTime = datetime.now()
            # # query.less_than('createdAt',curTime)
            #
            # questionNum = query.count()
            # print "[%s] total questionNums: %d in tableIndex: %s\n" %(datetime.now(),questionNum, tableIndexStr)
            # queryLimit = 700
            # queryTimes = questionNum/queryLimit + 1
            #
            # for index in range(queryTimes):
            #     query = Query(Question)
            #     # query.less_than('createdAt',Question)
            #     query.exists('questionId')
            #     query.descending('createdAt')
            #     query.limit(queryLimit)
            #     query.skip(index*queryLimit)
            #     query.select('questionId')
            #     query.select('tableIndex')
            #
            #     try:
            #         quesRet = query.find()
            #     except:
            #         try:
            #             quesRet = query.find()
            #         except:
            #             try:
            #                 quesRet = query.find()
            #             except:
            #                 quesRet = query.find()
            #
            #
            #     for ques in quesRet:
            #         quesInfoList =[]
            #         questionId = int(ques.get('questionId'))
            #         if questionId in self.questionIdSet :
            #             pass
            #         else:
            #
            #             client_s.incr('totalCount',1)
            #             client_s.incr('t'+tableIndexStr,1)
            #             quesInfoList.append(questionId)
            #             quesInfoList.append(int(ques.get('tableIndex')))
            #             self.questionIdSet.add(questionId)
            #             client_s.set(str(self.quesIndex),quesInfoList)
            #             self.quesIndex +=1


         # Questions = Object.extend('Questions')
         # query = Query(Questions)
         # query.exists('questionId')
         # curTime = datetime.now()
         # query.less_than('createdAt',curTime)
         #
         # questionNum = query.count()
         # print "questionNums: %s" %str(questionNum)
         # queryLimit = 500
         # queryTimes = questionNum/queryLimit + 1
         # self.urls = []
         # for index in range(queryTimes):
         #    query = Query(Questions)
         #    query.less_than('createdAt',curTime)
         #    query.exists('questionLinkHref')
         #    query.descending('createdAt')
         #    query.limit(queryLimit)
         #    query.skip(index*queryLimit)
         #    query.select('questionLinkHref')
         #    quesRet = query.find()
         #    for ques in quesRet:
         #        self.urls.append("http://www.zhihu.com"+ ques.get('questionLinkHref'))
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.stats)

    def start_requests(self):
        #print "start_requests ing ......"
        yield Request("http://www.zhihu.com",callback = self.post_login)

    def post_login(self,response):
        print "post_login ing ......"
        xsrfValue = response.xpath('/html/body/input[@name= "_xsrf"]/@value').extract()[0]
        yield FormRequest.from_response(response,
                                          #headers = self.headers,
                                          formdata={
                                              '_xsrf':xsrfValue,
                                              'email':'heamon9@163.com',
                                              'password':'heamon9@()',
                                              'rememberme': 'y'
                                          },
                                          dont_filter = True,
                                          callback = self.after_login
                                        #  dont_filter = True
                                          )

    def after_login(self,response):
        print "after_login ing ....."
        #inspect_response(response,self)
        #self.urls = ['http://www.zhihu.com/question/28626263','http://www.zhihu.com/question/22921426','http://www.zhihu.com/question/20123112']
        for questionId in self.questionIdSet:
            xsrfValue = response.xpath('/html/body/input[@name= "_xsrf"]/@value').extract()[0]
            yield FormRequest(url =self.baseUrl+str(questionId)+'/followers',
                                          #headers = self.headers,
                                          formdata={
                                              '_xsrf':xsrfValue,
                                              'start':0,
                                              'offset':20,
                                          },
                                          dont_filter = True,
                                          callback = self.parse
                                        #  dont_filter = True
                                          )


    def parse(self,response):
        inspect_response(response,self)

        if response.status != 200:
#            print "ParsePage HTTPStatusCode: %s Retrying !" %str(response.status)
            yield  self.make_requests_from_url(response.url)

        else:

            item =  ZhquesfollowerItem()








#             #print "parsePage ing......"
#             item['questionId'] = re.split('http://www.zhihu.com/question/',response.url)[1]
#             item['idZnonceContent'] = response.xpath('//*[@id="znonce"]/@content').extract()[0]  #right
#             item['dataUrlToken'] = response.xpath('//*[@id="zh-single-question-page"]/@data-urltoken').extract()[0] #right
#             item['isTopQuestion'] = response.xpath('//*[@id="zh-single-question-page"]/meta[@itemprop="isTopQuestion"]/@content').extract()[0]    #right
#             item['visitsCount'] = int(response.xpath('//*[@id="zh-single-question-page"]/meta[@itemprop="visitsCount"]/@content').extract()[0])    #right
#             try:
#                 item['tagLabelHrefList'] = response.xpath('//div[@id="zh-single-question-page"]//div[@class="zm-tag-editor-labels zg-clear"]/a/@href').extract()   #right
#                 item ['tagLabelDataTopicIdList'] = response.xpath('//div[@id="zh-single-question-page"]//div[@class="zm-tag-editor-labels zg-clear"]/a/@data-topicid').extract()   #right
#             except IndexError,e:
#                 item['tagLabelHrefList'] = []
#                 item['tagLabelDataTopicIdList'] =[]
#                 #print e
#
#             item['questionTitle'] = response.xpath('//div[@id="zh-question-title"]/h2/text()').extract()[0] #right
#
#             try:
#                 item['questionDetail'] = response.xpath('//div[@id="zh-question-detail"]/div[@class="zm-editable-content"]/text()').extract()
#                 if  item['questionDetail'] :
#                     item['questionDetail'] = item['questionDetail'][0]
#                 else:
#                     item['questionDetail'] = ''
#             except:
#                 item['questionDetail'] = response.xpath('//div[@id="zh-question-detail"]/textarea[@class="content hidden"]/text()').extract()[0]
#
#             item['dataResourceId'] = response.xpath('//div[@id="zh-question-detail"]/@data-resourceid').extract()[0]    #right
#
#
#             try:
#                 item['quesCommentCount'] = response.xpath('//div[@id="zh-question-meta-wrap"]//a[@name="addcomment"]/text()[2]').re('\d*')[0]   # should try
#                 if item['quesCommentCount']:
#                     item['quesCommentCount'] = int (item['quesCommentCount'])
#                 else:
#                     item['quesCommentCount'] = 0
#
#             except IndexError,e:
#                 item['quesCommentCount'] = 0
#
#             #item['quesCommentLink'] = response.xpath('').extract()
#             try:
#                 item['questionAnswerNum'] = int(response.xpath('//*[@id="zh-question-answer-num"]/@data-num').extract()[0])  #right
#             except IndexError,e:
#                 item['questionAnswerNum'] = 0
#
#             #item['dataPageSize'] = response.xpath('//*[@id="zh-question-answer-wrap"]').extract()
#             #item['pageSize'] = response.xpath('').extract()
#             #item['offset'] = response.xpath('').extract()
#             #item['nodeName'] = response.xpath('//*[@id="zh-question-answer-wrap"]/@nodename').extract()
#
#
#             # item['dataAid'] = response.xpath('//*[@id="zh-question-answer-wrap"]/div[1]').extract()
#             #
#             # item['dataAtoken'] = response.xpath('').extract()
#             # item['dataCreated'] = response.xpath('').extract()
#             # item['dataDeleted'] = response.xpath('').extract()
#             # item['dataHelpful'] = response.xpath('').extract()
#             # item['dataScore'] = response.xpath('').extract()
#
#             #item['questionFollowDataId'] = response.xpath('//*[@id="zh-question-side-header-wrap"]/button').extract()
#
#             #item['questionFollowerLink'] = response.xpath('//*[@id="zh-question-side-header-wrap"]/div[@class="zh-question-followers-sidebar"]/div[1]/a/@href').extract()[0]
#             try:
#                 item['questionFollowerCount'] = int(response.xpath('//*[@id="zh-question-side-header-wrap"]/div[@class="zh-question-followers-sidebar"]/div[1]/a/strong/text()').extract()[0])  #right
#             except IndexError,e:
#                 item['questionFollowerCount'] = 0
#
#             #item['quescommentcounttionFollowerList'] = response.xpath('//*[@id="zh-question-side-header-wrap"]/div[@class="zh-question-followers-sidebar"]/div[2]').extract()
#
#             #item['sideSectionId'] = response.xpath('//*[@id="shameimaru-question-up-83594d68c"]').extract()
#
#
#             #item['shareDataId'] = response.xpath('//*[@id="zh-question-webshare-container"]').extract()
#
#
#             # item[''] = response.xpath('').extract()
#             # item[''] = response.xpath('').extract()
#             # item[''] = response.xpath('').extract()
#
#
#             item['questionLatestActiveTime'] = response.xpath('//*[@id="zh-single-question-page"]//span[@class="time"]/text()').extract()[0]
#            # item['questionLog'] = response.xpath('//*[@id="zh-single-question-page"]/div[2]/div[5]/div/div[1]/a').extract()[0]
#             try:
#                 item['questionShowTimes'] = int(response.xpath('//*[@id="zh-single-question-page"]/div[@class="zu-main-sidebar"]/div[last()-1]//div[@class="zg-gray-normal"][2]/strong[1]/text()').extract()[0])
#             except:
#                 item['questionShowTimes'] = int(response.xpath('//*[@id="zh-single-question-page"]/div[@class="zu-main-sidebar"]/div[last()]//div[@class="zg-gray-normal"][2]/strong[1]/text()').extract()[0])
#
#             try:
#                 item['topicRelatedFollowerCount'] = int(response.xpath('//*[@id="zh-single-question-page"]/div[@class="zu-main-sidebar"]/div[last()-1]//div[@class="zg-gray-normal"][2]/strong[2]/text()').extract()[0])
#             except:
#                 item['topicRelatedFollowerCount'] = int(response.xpath('//*[@id="zh-single-question-page"]/div[@class="zu-main-sidebar"]/div[last()]//div[@class="zg-gray-normal"][2]/strong[2]/text()').extract()[0])
#            # item['questionFollowerCount'] = response.xpath('//*[@id="zh-question-side-header-wrap"]/div[2]/div[1]/a/strong').extract()[0]
#
#
#             try:
#                 item['relatedQuestionLinkList'] = response.xpath('//*[@id="zh-question-related-questions"]//ul//li//a/@href').extract()     #should try
#             except IndexError,e:
#                 item['relatedQuestionLinkList'] = []
#
#
#
#           #  print response.status
            yield item
#
#         # for index,url in enumerate(self.urls):
#         #     yield Request(url,meta = {'cookiejar':index},callback = self.parse_page)
#



    # def closed(self,reason):
    #     #f = open('../../nohup.out')
    #     #print f.read()
    #     leancloud.init(settings.APP_ID, master_key=settings.MASTER_KEY)
    #
    #
    #     CrawlerLog = Object.extend('CrawlerLog')
    #     crawlerLog = CrawlerLog()
    #
    #     crawlerLog.set('crawlerName',self.name)
    #     crawlerLog.set('closedReason',reason)
    #     crawlerLog.set('crawlerStats',self.stats.get_stats())
    #     try:
    #         crawlerLog.save()
    #     except:
    #         pass
    #
    #
