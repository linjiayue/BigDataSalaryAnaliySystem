#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#爬取拉钩网全国站，指定关键词的信息
import time,sys,logging,logging.config,parser,json,os,re
from urllib import request
from datetime import datetime,timedelta
##########################################
#设置日志格式
def setLog():
    logging.config.fileConfig('logging.conf')
    logger = logging.getLogger('cycleRent')
    return logger
logger = setLog()
###########################################

#传入查询城市以及关键词
def search(keyword,filename,isFist):
    #需要查询的url
    url = "http://www.lagou.com/jobs/positionAjax.json?"
    #添加关键词查询
    logger.info("添加城市查询条件，关键字为:"+ keyword)
    url = url + "&kd="+request.quote(keyword)
    #第一次查询，获取总记录数
    url = url + "&first=true&pn=1"
    logger.info("第一次查询的URL为："+url)
    #查询到的总条数
    totalCount = 0
    #记录总页数
    totalPage = 0

    with request.urlopen(url) as resultF:
        if resultF.status == 200 and resultF.reason == 'OK':
            #获得所返回的记录
            data = resultF.read().decode('utf-8')
            #将data进行JSON格式化
            dataJson = json.loads(data)
            #获得总记录数
            totalCount = int(dataJson['content']['totalCount'])
            #获得总页数
            totalPage = int(dataJson['content']['totalPageCount'])
            logger.info("查询出的总记录数为："+ str(totalCount))
            logger.info("查询出的总页数为："+ str(totalPage))
            logger.info("查询出的每页总数为："+ str(dataJson['content']['pageSize']))
        else:
            logger.error("第一次打开url出错,没有正常连接，返回代码为："+str(resultF.status))
            #结束程序
            return
    #查询的当前日期
    dateStr = datetime.now().strftime('%Y%m%d')
    #根据总页数进行循环读取
    for i in range(totalPage):
        #转码操作
        if i == 0 :
            url = "http://www.lagou.com/jobs/positionAjax.json?" + "&kd="+request.quote(keyword)+"&first=true&pn=1"
        else:
            url = "http://www.lagou.com/jobs/positionAjax.json?" + "&kd="+request.quote(keyword)+"&first=false&pn="+str((i+1))
        logger.info("查询的URL为："+url)
        #模拟谷歌浏览器发送请求
        req = request.Request(url)
        req.add_header("User-Agent","Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.132 Safari/537.36")
        with request.urlopen(req) as result:
            if resultF.status == 200 and resultF.reason == 'OK':
                #获得所返回的记录
                data = str(result.read().decode('utf-8'))
                #增加日期区分，以及记录总条数，ex 20151210_981;内容
                #将data进行JSON格式化
                dataJson = json.loads(data, encoding='utf-8')
                saveDataForHbase(dataJson['content']['result'], filename, isFist)
        #访问每条链接，休眠1秒，防止被反爬虫
        time.sleep(1)

#保存数据到指定文件中
def saveDataForHbase(dataJson, filename, isFist):

    #增加该文件为Hbaase标志
    filename = filename + '_hb' + '.txt'
    logger.info("Hbase所存储的文件名为："+str(filename))
    with open(str(filename), 'a', encoding='utf-8') as rData:
        for data in dataJson:
            #如果不是第一次爬取，并且该数据的时间不是爬取日期时间，则不存入文件中
            if not isFist and not isYestoday(getForMatTime(str(data['createTime']), '%Y-%m-%d %H:%M:%S', '%Y-%m-%d'), '%Y-%m-%d'):
                continue
            #转化为标准json字符串存入文件中
            rData.write(json.dumps(data, ensure_ascii=False))
            rData.write("\n")

#保存数据到Hive的指定文件中
def saveDataForHive(dataJson, filename, isFist):
    filename = filename + '_hi' + '.txt'
    logger.info("Hive所存储的文件名为："+str(filename))
    with open(filename, 'a' ,encoding='utf-8') as rData:
        for data in dataJson:
            #如果不是第一次爬取，并且该数据的时间不是爬取日期时间，则不存入文件中
            if not isFist and not isYestoday(getForMatTime(str(data['createTime']), '%Y-%m-%d %H:%M:%S', '%Y-%m-%d'), '%Y-%m-%d'):
                continue
            rStr = ''.join([str(data['positionName']), '&'+str(data['createTime']), '&'+str(data['salary']), '&'+str(data['workYear']), '&'+str(data['city']), '&'+str(data['createTimeSort']), '&'+str(data['companySize']),
                            '&'+str(data['financeStage']), '&'+str(data['education']), '&'+str(data['positionAdvantage'])
                            , '&'+str(data['positionType']), '&'+str(data['industryField']), '&'+str(data['companyLabelList']), '&'+str(data['companyName']), '&'+str(data['companyShortName']),
                            '&'+str(data['jobNature']), '&'+str(data['positionFirstType']), '&'+str(data['leaderName']), '&'+str(data['flowScore']),
                            '&'+str(data['searchScore']), '&'+str(data['countAdjusted']), '&'+str(data['pvScore'])
                            , '&'+str(data['companyLogo']), '&'+str(data['positonTypesMap']), '&'+str(data['orderBy']), '&'+str(data['formatCreateTime']), '&'+str(data['haveDeliver']), '&'+str(data['adWord']),
                            '&'+str(data['score']), '&'+str(data['positionId']), '&'+str(data['deliverCount']), '&'+str(data['relScore']), '&'+str(data['totalCount']),
                            '&'+str(data['showOrder']), '&'+str(data['showCount'])
                            , '&'+str(data['calcScore']), '&'+str(data['companyId']), '&'+str(data['randomScore']), '&'+str(data['hrScore']), '&'+str(data['adjustScore']),
                            '&'+str(data['imstate']), '&'+str(data['plus'])])
            rData.write(rStr)
            rData.write("\n")

#判断所传入的时间字符串是不是当天日期
#dateTimeStr 时间字符串,dateTimeFormat 时间字符串格式
def isNowDate(dateTimeStr,dateTimeFormat):
    nowDateStr = datetime.now().strftime(dateTimeFormat)
    if nowDateStr == dateTimeStr:
        return True
    else:
        return False

#判断传入的日期是否是昨天的日期
def isYestoday(dateTimeStr,dateTimeFormat):
    nowDay = datetime.now()
    oneDay = timedelta(days=1)
    yestoday = nowDay - oneDay
    yesDateStr = yestoday.strftime("%Y-%m-%d")
    if dateTimeStr == yesDateStr:
        return True
    return False

#按照指定格式返回字符串
#dateTimeStr 时间字符串,dateTimeFormat 时间字符串格式,toDateTimeFormat 转化的时间格式
def getForMatTime(dateTimeStr,dateTimeFormat,toDateTimeFormat):
    dt = datetime.strptime(dateTimeStr,dateTimeFormat)
    return dt.strftime(toDateTimeFormat)

#获得从当前时间到明日的具体秒数
def getSleepSeconds():
    dateNow = datetime.now()
    dataNowStr = dateNow.strftime('%Y%m%d')
    #获得明日日期
    dataN = dateNow + timedelta(days=1)
    dataNStr = dataN.strftime('%Y%m%d')
    dataNTime = datetime.strptime(dataNStr+'000000', '%Y%m%d%H%M%S')
    #计算差异时间
    dataC = dataNTime - dateNow
    return dataC.seconds



def exeCmd(cmd):
    resultStr = os.popen(cmd).read()
    return resultStr

#创建指定文件夹公用方法
def createDirUtil(dirStr):
    if not os.path.exists(dirStr):
        logger.info("创建了文件夹："+ dirStr)
        os.mkdir(dirStr)

#运行加载数据的Jar包
def runJarForHbase(filePath):
    cmd = "java -jar /home/hadoop/runJar/DataToHbase.jar " + filePath
    logger.info("runJarForHbase执行命令:" + str(cmd))
    os.popen(cmd)

#运行Hive脚本加载数据到表中
def runShellForHive(filePath):
    cmd = "hive -e \"LOAD DATA LOCAL INPATH '" + filePath + "' INTO TABLE SalaryInfo\""
    logger.info("runShellForHive执行命令:" + str(cmd))
    os.popen(cmd)

#格式化字符串，避免数据中出现 {'positionName': '数据分析工程师助理','positionAdvantage': "技术氛围好，大牛领队，rmbp+27'苹果显示器", 'plus': '否'}
#后无法转换为json对象的错误
#data   需要转换的字符串
def formatStr(data):
    myRe = re.compile(r'"(.*)"')
    strs = myRe.findall(data)
    for str in strs:
        data = data.replace(str, str.replace("'", " "))
    return data

if __name__ == '__main__':
    try:
        #保存是否是第一次的标志
        isFist = True
        #接收输入的参数值，多个参数以 ; 分割
        #inputKeyword = str(input("请输入需要爬取的关键字："))
        #keyword = inputKeyword.split(";")
        #需要查询的关键词
        keyword = ['Hadoop', '数据挖掘', '数据分析', 'Spark']
        #创建关键词对应文件字典
        filePaths = {'Hadoop': 'Hadoop', '数据挖掘': 'DataMining', '数据分析': 'DataAnalySis', 'Spark': 'Spark'}
        dirStr = "/home/hadoop/crawlerData/"
        createDirUtil(dirStr)

        #查询的当前日期
        dateStr = datetime.now().strftime('%Y%m%d')
        #拼装当前查询时间存储文件夹
        dirStr = dirStr + '/' + dateStr
        createDirUtil(dirStr)

        while True:
            logger.info("程序启动：")
            logger.info("关键词内容抽取启动：")
            for k in keyword:
                search(k, dirStr + '/' + filePaths[k])
                #调用Jar包
                runJarForHbase(dirStr + '/' + filePaths[k] + '_hb' + '.txt')

            #将是否是第一次的标志置为 False
            isFist = False

            logger.info("关键词内容抽取结束：")

            logger.info("程序跑完了~~~")
            seconds = getSleepSeconds()
            logger.info("开始休眠，休眠时间为："+str(seconds))
            time.sleep(seconds)
            logger.info("休眠结束~~~~")
    except BaseException as b:
        logger.error("程序出错，错误原因："+ str(b))
