# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

g
from scrapy.exceptions import DropItem
from zhQuesFollower import settings

import re
import time
import happybase
import redis
import logging

class FollowerPipeline(object):
    # dbPrime1 = 97
    # dbPrime2 = 997

    def __init__(self):
        # leancloud.init(settings.APP_ID, master_key=settings.MASTER_KEY)
        self.redis1 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=1)

        #redis3存放用户索引，linkid，dataid，index
        self.redis3 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=3)
        #redis4存放用户的基础信息
        self.redis4 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=4)
        #redis5存放问题的关注者集合
        self.redis5 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=5)
        connection = happybase.Connection(settings.HBASE_HOST)
        self.userTable = connection.table('user')


#这里简单处理，不考虑关注者的前后顺序，处理为一个集合,每个关注在数据库里存为一条记录，在缓存里存为一个hash表
    def process_item(self, item, spider):
        if item['userDataId'] :
            questionId = str(item['questionId'])
            currentTimestamp = int(time.time())

            userDataId= item['userDataId']
            #userLinkId可能有中文
            userLinkId = item['userLinkList'].encode('utf-8')




            #如果成功赋值，返回1，说明该用户当前没有上锁，可更新
            if  self.redis3.hsetnx('userLock','userDataId',1):
                #通过查询dataId，判断是否记录过该用户信息
                result = self.redis4.lrange(str(userDataId),0,1)
                if result:
                    [recordTimestamp,userIndex]=result
                else:
                    [recordTimestamp,userIndex]=('','')
                # 表示数据库并没有该用户的信息
                if not recordTimestamp:
                    userIndex = self.redis3.incr('totalCount',1)
                    #这里有一点小问题，假设了下面的p3不会失败，一旦失败可能会有问题，原子性质
                    p3 = self.redis3.pipeline()
                    p3.hset('userIndex',str(userIndex),userDataId)
                    #为了方便一次取得所有用户的linkid，这个地方比较复杂，要考虑userLinkId在不同时期被不同用户重用的情况，以及改名
                    #这里是为了方便用userIndex取代userDataId
                    p3.hset('userIndexLinkId',userIndex,str(userLinkId))
                    p3.hset('userDataLinkId',userDataId,str(userLinkId))
                    #解除锁定
                    p3.hdel('userLock',userDataId)
                    p3.execute()
                else:
                    #解除锁定
                    self.redis3.hdel('userLock',userDataId)
            else:
                #另外一个client可能已将用户上锁，但还没来得及更新用户的userIndex

                result = self.redis4.lrange(str(userDataId),0,1)
                if result:
                    [recordTimestamp,userIndex]=result
                else:
                    result = self.redis4.lrange(str(userDataId),0,1)
                    if result:
                        [recordTimestamp,userIndex]=result
                    else:
                        result = self.redis4.lrange(str(userDataId),0,1)
                        if result:
                            [recordTimestamp,userIndex]=result
                        else:
                            #说明这个用户有异常，本次丢弃
                            [recordTimestamp,userIndex]=('','')
                            #这里是会直接返回吗
                            DropItem()
            #这里在做的是一直想得到用户的userIndex，而避免使用字节数很长的userDataId
            #将该用户添加到相应的问题集合中，这里使用userIndex未必合适
            #可能最后是空值吗？
            self.redis5.sadd(str(questionId),str(userIndex))

            #如果没有记录过该用户，或者上条Hbase里数据库记录的时间超过了5天
            #这是为了避免几个爬虫爬到同一个用户，这个数据需要根据爬虫的效率更改
            if not recordTimestamp or (int(currentTimestamp)-int(recordTimestamp)>int(settings.UPDATE_PERIOD)): # 最多5天更新一次
                recordTimestamp = currentTimestamp
                try:
                    self.userTable.put(str(userDataId)
                                   ,{'basic:dataId':str(userDataId)
                                    ,'basic:linkId':str(userLinkId)
                                     ,'basic:imgUrl':str(item['userImgUrl'])
                                     ,'basic:name':item['userNamet'].encode('utf-8')
                                     ,'basic:answerCount':str(item['userAnswerCount'])
                                     ,'basic:askCount':str(item['userAskCount'])
                                     ,'basic:followerCount':str(item['userFollowerCount'])
                                     ,'basic:upCount':str(item['userUpCount'])})
                except Exception,e:
                    logging.error('Error with put questionId into hbase: '+str(e)+' try again......')
                    try:
                        self.userTable.put(str(userDataId)
                                   ,{'basic:dataId':str(userDataId)
                                    ,'basic:linkId':str(userLinkId)
                                     ,'basic:imgUrl':str(item['userImgUrl'])
                                     ,'basic:name':item['userNamet'].encode('utf-8')
                                     ,'basic:answerCount':str(item['userAnswerCount'])
                                     ,'basic:askCount':str(item['userAskCount'])
                                     ,'basic:followerCount':str(item['userFollowerCount'])
                                     ,'basic:upCount':str(item['userUpCount'])})
                        logging.error(' tried again and successfully put data into hbase ......')
                    except Exception,e:
                        logging.error('Error with put questionId into hbase: '+str(e)+'tried again and failed')


                p4 = self.redis4.pipeline()
                p4.lpush(str(userDataId)

                                  ,str(userLinkId)
                                  ,item['userImgUrl']
                                  ,item['userName']
                                  ,item['userAnswerCount']
                                  ,item['userAskCount']
                                  ,item['userFollowerCount']
                                  ,item['userUpCount']
                                  ,str(userIndex)
                                  ,str(recordTimestamp))
                #为了减少redis请求次数
                p4.ltrim(str(userDataId),0,8)
                p4.execute()


        DropItem()
            # questionTimestamp = self.redis1.lindex(questionIdStr,2)
            #
            # tableIndex1 = int(questionTimestamp)%self.dbPrime1
            # tableIndex2 = int(questionTimestamp)%self.dbPrime2
            # if tableIndex1 <10:
            #     tableIndexStr1 = '0'+str(tableIndex1)
            # else:
            #     tableIndexStr1 = str(tableIndex1)
            # if tableIndex2 <10:
            #     tableIndexStr2 =   '00'+str(tableIndex2)
            # elif tableIndex2 <100:
            #     tableIndexStr2 =   '0'+str(tableIndex2)
            # else:
            #     tableIndexStr2 =   str(tableIndex2)
            # tableIndexStr = tableIndexStr1 + tableIndexStr2
 # item['questionId'] = re.split('http://www.zhihu.com/question/(\d*)/followers',response.url)[1]
 #                item['userDataIdList'] = sel.xpath('//button/@data-id').extract()
 #                item['userLinkList'] = sel.xpath('//a[@class="zm-item-link-avatar"]/@href').extract()
 #                item['userImgUrlList'] = sel.xpath('//a[@class="zm-item-link-avatar"]/img/@src').extract()
 #                item['userNameList'] = sel.xpath('//h2/a/text()').extract()
 #                item['userFollowersList'] = sel.xpath('//div[@class="details zg-gray"]/a[1]//text()').extract()
 #                item['userAskList'] = sel.xpath('//div[@class="details zg-gray"]/a[2]//text()').extract()
 #                item['userAnswerList'] = sel.xpath('//div[@class="details zg-gray"]/a[3]//text()').extract()
 #                item['userUpList'] = sel.xpath('//div[@class="details zg-gray"]/a[4]//text()').extract()



                # else:
                #     if self.redis3.sadd(userDataIdStr,userLinkId):
                #         self.redis3.hset('userDataId',userDataIdStr,userLinkId) #要不要记录用户的userLinkId更改时间这个信息
                #     userIndex = self.redis3.hget('userDataIdIndex',userDataIdStr)

                    # self.redis5.incr('totalCount',1)

                    # QuestionFollower = Object.extend('QuesFollow'+tableIndexStr)
                    # questionFollower = QuestionFollower()
                    #
                    #
                    #
                    # questionFollower.set('tableIndexStr',tableIndexStr)
                    # questionFollower.set('questionId',questionIdStr)
                    # questionFollower.set('userIndex',str(userIndex))
                    # questionFollower.set('userDataId',userDataIdStr)
                    # questionFollower.set('userLinkId',userLinkId)













