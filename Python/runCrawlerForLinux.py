#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#调用脚本，已启动爬取服务
import os,logging,logging.config
##########################################
#设置日志格式
def setLog():
    logging.config.fileConfig('logging.conf')
    logger = logging.getLogger('cycleRent')
    return logger
logger = setLog()
###########################################
if __name__ == "__main__":
    logger.info("调用启动爬取脚本.....")
    os.popen("/home/hadoop/pythonTest/runCrawler.sh")
    logger.info("调用启动爬取脚本结束.....")