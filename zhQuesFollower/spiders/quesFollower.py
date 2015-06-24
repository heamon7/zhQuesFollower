# -*- coding: utf-8 -*-
import scrapy

from scrapy.spider import BaseSpider
from scrapy.selector import HtmlXPathSelector
from scrapy.http import Request,FormRequest
from scrapy.conf import settings
from scrapy.selector import Selector
from scrapy import log
from scrapy.shell import inspect_response


import leancloud
from leancloud import Object
from leancloud import LeanCloudError
from leancloud import Query

from datetime import datetime
from zhQuesFollower import settings

from zhQuesFollower.items import ZhquesfollowerItem
import bmemcached
import re

import json
import redis
import happybase
import requests

class QuesfollowerSpider(scrapy.Spider):
    name = "quesFollower"
    allowed_domains = ["www.zhihu.com"]
    baseUrl = "http://www.zhihu.com/question/"
    start_urls = (
        'http://www.zhihu.com/',
    )
    questionIdList = []
    questionFollowerCountList = []
    questionInfoList = []
    quesIndex =0
    reqLimit =20
    pipelineLimit = 100000
    threhold = 100
    handle_httpstatus_list = [401,429,500,502,504]
    #handle_httpstatus_list = [401,429,500]





    def __init__(self,spider_type='Master',spider_number=0,partition=1,**kwargs):

        # self.stats = stats
        print "Initianizing ....."
        self.redis0 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_USER+':'+settings.REDIS_PASSWORD,db=0)
        self.redis2 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_USER+':'+settings.REDIS_PASSWORD,db=2)
        self.spider_type = str(spider_type)
        self.spider_number = int(spider_number)
        self.partition = int(partition)
        self.email= settings.EMAIL_LIST[self.spider_number]
        self.password=settings.PASSWORD_LIST[self.spider_number]

        #log.start()
        # leancloud.init(settings.APP_ID_S, master_key=settings.MASTER_KEY_S)


        # client_2 = bmemcached.Client(settings.CACHE_SERVER_2,settings.CACHE_USER_2,settings.CACHE_PASSWORD_2)
        # client_4 = bmemcached.Client(settings.CACHE_SERVER_4,settings.CACHE_USER_4,settings.CACHE_PASSWORD_4)
        #


        # dbPrime = 97


        # for questionId in self.questionIdList:
        #     print "askfor followerCount %s"  %str(questionId)
        #     self.questionFollowerCountList.ex(redis2.lindex(str(questionId),4))

        # dbPrime = 97
        # totalCount = int(client_2.get('totalCount'))
        # for questionIndex in range(0,totalCount):
        #     self.questionIdSet.add(int(client_2.get(str(questionIndex))[0]))

            #貌似这样占用的内存太多了
        #这里要获得问题的id及其关注者的数量
        #因为不能一次获得所有的列表，所以需要分次

                # print "length of questionFollowerCountList: %s\n" %str(len(self.questionFollowerCountList))





            # if questionInfo:
            #     if int(questionInfo[4])>self.threhold:
            #
            #         self.questionIdList.append([questionId,questionInfo[4]])
            #     else:
            #         pass
            # else:
            #     pass

        # self.questionInfoList.append([20769127,838])


    # @classmethod
    # def from_crawler(cls, crawler):
    #     return cls(crawler.stats)

    def start_requests(self):

        self.questionIdList = self.redis2.keys()

        p2 = self.redis2.pipeline()


        # self.questionIdList = self.redis0.hvals('questionIndex')
        questionIdListLength = len(self.questionIdList)

        if self.spider_type=='Master':
            log.msg('Master spider_type is '+self.spider_type,level=log.WARNING)
            if self.partition!=1:
                log.msg('Master non 1 partition is '+str(self.partition),level=log.WARNING)
                self.questionIdList = self.questionIdList[self.spider_number*questionIdListLength/self.partition:(self.spider_number+1)*questionIdListLength/self.partition]

                for index ,questionId in enumerate(self.questionIdList):
                    p2.lindex(str(questionId),6)
                    if index%self.pipelineLimit ==0:
                        self.questionFollowerCountList.extend(p2.execute())
                    elif questionIdListLength-index==1:
                        self.questionFollowerCountList.extend(p2.execute())                        
                    # p2 = self.redis2.pipeline()

                for index in range(1,self.partition):
                    payload ={
                        'project':settings.BOT_NAME
                        ,'spider':self.name
                        ,'spider_type':'Slave'
                        ,'spider_number':index
                        ,'partition':self.partition
                        ,'setting':'JOBDIR=/tmp/scrapy/'+self.name+str(index)
                    }
                    log.msg('Begin to request'+str(index),level=log.WARNING)
                    response = requests.post('http://'+settings.SCRAPYD_HOST_LIST[self.spider_number]+':'+settings.SCRAPYD_PORT+'/schedule.json',data=payload)
                    log.msg('Response: '+str(index)+' '+str(response),level=log.WARNING)
            else:
                log.msg('Master  partition is '+str(self.partition),level=log.WARNING)
                for index ,questionId in enumerate(self.questionIdList):
                    p2.lindex(str(questionId),6)
                    if index%self.pipelineLimit ==0:
                        self.questionFollowerCountList.extend(p2.execute())
                    elif questionIdListLength-index==1:
                        self.questionFollowerCountList.extend(p2.execute())

        elif self.spider_type =='Slave':
            log.msg('Slave spider_type is '+self.spider_type,level=log.WARNING)
            log.msg('Slave number is '+str(self.spider_number) + ' partition is '+str(self.partition),level=log.WARNING)
            if (self.partition-self.spider_number)!=1:
                self.questionIdList = self.questionIdList[self.spider_number*questionIdListLength/self.partition:(self.spider_number+1)*questionIdListLength/self.partition]

                for index ,questionId in enumerate(self.questionIdList):
                    p2.lindex(str(questionId),6)
                    if index%self.pipelineLimit ==0:
                        self.questionFollowerCountList.extend(p2.execute())
                    elif questionIdListLength-index==1:
                        self.questionFollowerCountList.extend(p2.execute())
                    # p2 = self.redis2.pipeline()

            else:
                self.questionIdList = self.questionIdList[self.spider_number*questionIdListLength/self.partition:]
                for index ,questionId in enumerate(self.questionIdList):
                    p2.lindex(str(questionId),6)
                    if index%self.pipelineLimit ==0:
                        self.questionFollowerCountList.extend(p2.execute())
                    elif questionIdListLength-index==1:
                        self.questionFollowerCountList.extend(p2.execute())
                    # p2 = self.redis2.pipeline()
        else:
            log.msg('spider_type is:'+str(self.spider_type)+'with type of '+str(type(self.spider_type)),level=log.ERROR)

        log.msg('start_requests ing ......',level=log.WARNING)
        yield Request("http://www.zhihu.com/",callback = self.post_login)

    def post_login(self,response):

        log.msg('post_login ing ......',level=log.WARNING)
        xsrfValue = response.xpath('/html/body/input[@name= "_xsrf"]/@value').extract()[0]
        yield FormRequest.from_response(response,
                                          #headers = self.headers,
                                          formdata={
                                              '_xsrf':xsrfValue,
                                              'email':self.email,
                                              'password':self.password,
                                              'rememberme': 'y'
                                          },
                                          dont_filter = True,
                                          callback = self.after_login,
                                          )





    def after_login(self,response):

        log.msg('after_login ing .....',level=log.WARNING)
        # print self.questionInfoList
        #inspect_response(response,self)
        #inspect_response(response,self)
        #self.urls = ['http://www.zhihu.com/question/28626263','http://www.zhihu.com/question/22921426','http://www.zhihu.com/question/20123112']
        for index ,questionId in enumerate(self.questionIdList):
            if self.questionFollowerCountList[index]:
                xsrfValue = response.xpath('/html/body/input[@name= "_xsrf"]/@value').extract()[0]

                reqUrl = self.baseUrl+str(questionId)+'/followers'

                reqTimes = (int(self.questionFollowerCountList[index])+self.reqLimit-1)/self.reqLimit
                for index in reversed(range(reqTimes)):
                    # print "request index: %s"  %str(index)
                    offset =str(self.reqLimit*index)
                    yield FormRequest(url =reqUrl
                                      ,meta={'xsrfValue':xsrfValue,'offset':str(offset)}
                                              #headers = self.headers,

                                      , formdata={
                                            '_xsrf': xsrfValue,
                                            'start': '0',
                                            'offset': str(offset),
                                        }
                                      ,dont_filter=True
                                      ,callback=self.parsePage
                                      )


    def parsePage(self,response):


        if response.status != 200:
            # print "ParsePage HTTPStatusCode: %s Retrying !" %str(response.status)
            # print "ParsePage HTTPStatusCode: %s Retrying !" %str(response.status)
            yield FormRequest(url =response.request.url
                                      ,meta={'xsrfValue':response.meta['xsrfValue'],'offset':response.meta['offset']}
                                              #headers = self.headers,

                                      , formdata={
                                            '_xsrf': response.meta['xsrfValue'],
                                            'start': '0',
                                            'offset':str(response.meta['offset']),
                                        }
                                      ,dont_filter=True
                                      ,callback=self.parsePage
                                      )
        else:

            item =  ZhquesfollowerItem()

    #         if response.status != 200:
    # #            print "ParsePage HTTPStatusCode: %s Retrying !" %str(response.status)
    #             yield  self.make_requests_from_url(response.url)
    #
    #         else:

           # inspect_response(response,self)
            data = json.loads(response.body)
            userCountRet = data['msg'][0]
            # print "userCountRet: %s" %userCountRet
            #这里注意要处理含有匿名用户的情况
            if userCountRet:
                sel = Selector(text = data['msg'][1])
                #item['offset'] = response.meta['offset']
                item['questionId'] = re.split('http://www.zhihu.com/question/(\d*)/followers',response.url)[1]
                item['userDataIdList'] = sel.xpath('//button/@data-id').extract()
                item['userLinkList'] = sel.xpath('//a[@class="zm-item-link-avatar"]/@href').extract()
                item['userImgUrlList'] = sel.xpath('//a[@class="zm-item-link-avatar"]/img/@src').extract()
                item['userNameList'] = sel.xpath('//h2/a/text()').extract()
                item['userFollowersList'] = sel.xpath('//div[@class="details zg-gray"]/a[1]//text()').re(r'(^\d+)')
                item['userAskList'] = sel.xpath('//div[@class="details zg-gray"]/a[2]//text()').re(r'(^\d+)')
                item['userAnswerList'] = sel.xpath('//div[@class="details zg-gray"]/a[3]//text()').re(r'(^\d+)')
                item['userUpList'] = sel.xpath('//div[@class="details zg-gray"]/a[4]//text()').re(r'(^\d+)')
            else:
                item['userDataIdList']=''

            yield item





    #
    #
    def closed(self,reason):

        redis5 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_USER+':'+settings.REDIS_PASSWORD,db=5)
        connection = happybase.Connection(settings.HBASE_HOST)
        questionTable = connection.table('testquestion')


        # pipelineLimit = 1000

        p5 = redis5.pipeline()
        for index ,questionId in enumerate(self.questionIdList):
            p5.smembers(str(questionId))
            if index%self.pipelineLimit ==0:
                for followerList in p5.execute():
                    questionTable.put(str(questionId),{'follower:list':str(list(followerList))})
            elif len(self.questionIdList)-index==1:
                for followerList in p5.execute():
                    questionTable.put(str(questionId),{'follower:list':str(list(followerList))})

        log.msg('Finish All.....',level=log.WARNING)

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


