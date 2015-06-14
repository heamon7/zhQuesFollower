# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import leancloud
from leancloud import Object
from leancloud import LeanCloudError
from leancloud import Query
from scrapy import log
from scrapy.exceptions import DropItem
from zhQuesFollower import settings
import bmemcached
import re

import redis
class FollowerPipeline(object):
    dbPrime1 = 97
    dbPrime2 = 101

    def __init__(self):
        leancloud.init(settings.APP_ID, master_key=settings.MASTER_KEY)
        self.redis1 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_USER+':'+settings.REDIS_PASSWORD,db=1)
        self.redis3 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_USER+':'+settings.REDIS_PASSWORD,db=3)
        self.redis4 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_USER+':'+settings.REDIS_PASSWORD,db=4)
        self.redis5 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_USER+':'+settings.REDIS_PASSWORD,db=5)



#这里简单处理，不考虑关注者的前后顺序，处理为一个集合,每个关注在数据库里存为一条记录，在缓存里存为一个hash表
    def process_item(self, item, spider):
        if item['questionId']:
            questionIdStr = str(item['questionId'])
            questionTimestamp = self.redis1.lindex(questionIdStr,2)

            tableIndex1 = questionTimestamp%self.dbPrime1
            tableIndex2 = questionTimestamp%self.dbPrime2
            if tableIndex1 <10:
                tableIndexStr1 = '0'+str(tableIndex1)
            else:
                tableIndexStr1 = str(tableIndex1)
            if tableIndex2 <10:
                tableIndexStr2 =   '0'+str(tableIndex2)
            else:
                tableIndexStr2 =   str(tableIndex2)
            tableIndexStr = tableIndexStr1 + tableIndexStr2



            for index , userDataId in enumerate(item['userDataIdList']):
                userDataIdStr= str(item['userDataIdList'][index])
                userLinkId = re.split('http://www.zhihu.com/people/',item['userLinkList'][index])[1]

                if self.redis3.hsetnx('userDataId',userDataIdStr,userLinkId):
                    userIndex = self.redis3.incr('totalCount',1)
                    p3 = self.redis3.pipeline()
                    p3.hset('userLinkId',userLinkId,userDataIdStr)        #这个地方比较复杂，要考虑userLinkId被重用的情况，以及改名
                    p3.hset('userIndex',str(userIndex),userDataIdStr)
                    p3.hset('userDataIdIndex',userDataIdStr,int(userIndex))
                    p3.execute()

                    p4 = self.redis4.pipeline()
                    p4.rpush(userDataIdStr,userIndex,userLinkId,item['userImgUrlList'][index],item['userNameList'][index],
                             item['userAnswerList'][index],item['userAskList'][index],item['userFollowersList'][index],
                             item['userUpList'][index])
                else:
                    if self.redis3.sadd(userDataIdStr,userLinkId):
                        self.redis3.hset('userDataId',userDataIdStr,userLinkId) #要不要记录用户的userLinkId更改时间这个信息
                    userIndex = self.redis3.hget('userDataIdIndex',userDataIdStr)

                if self.redis5.sadd(str(questionIdStr),userIndex):
                    self.redis5.incr('totalCount',1)

                    QuestionFollower = Object.extend('QuestionFollower'+tableIndexStr)
                    questionFollower = QuestionFollower()



                    questionFollower.set('tableIndexStr',tableIndexStr)
                    questionFollower.set('questionId',questionIdStr)
                    questionFollower.set('userIndex',userIndex)
                    questionFollower.set('userDataId',userDataIdStr)
                    questionFollower.set('userLinkId',userLinkId)
                    try:
                        questionFollower.save()
                    except LeanCloudError,e:
                        try:
                            questionFollower.save()
                        except LeanCloudError,e:
                            print e




        DropItem()
