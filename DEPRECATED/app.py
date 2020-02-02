from flask import Flask, Response, request, json
import requests
import os
from os import path
import asyncio
from crawler import GrapPage
from threading import Thread
import time
from utils.util import logger
app = Flask(__name__)
dir = path.dirname(__file__)
PORT = 5001

isCrawling = False


def startCrawl():
    global isCrawling
    isCrawling = True
    ins = GrapPage('sh', 'https://sh.zu.ke.com/zufang')
    try:
        ins.start()
    finally:
        isCrawling = False
        ins.quit()


def startFillInfo():
    global isCrawling
    isCrawling = True
    ins = GrapPage('sh', 'https://sh.zu.ke.com/zufang')
    try:
        ins.start_filling_missing_info()
    finally:
        isCrawling = False
        ins.quit()


def startAsyncTask(target):
    try:
        time.sleep(1)
        thr = Thread(target=target)
        thr.start()
    finally:
        isCrawling = False


def asyncStartCrawl():
    startAsyncTask(startCrawl)


def asyncStartFillEmpty():
    startAsyncTask(startFillInfo)


def asyncStartCrawlLatest():
    def crawlLatest():
        global isCrawling
        isCrawling = True
        ins = GrapPage('sh', 'https://sh.zu.ke.com/zufang')
        try:
            ins.start_by_latest()
        finally:
            isCrawling = False
            ins.quit()
    startAsyncTask(crawlLatest)


@app.route('/', methods=['GET'])
def get_():
    return Response('Hola')


@app.route('/start', methods=['GET'])
def get_start():
    token = request.args.get('token')
    if(token != 'q1w2e3r4'):
        return Response('Not Found'), 404
    _isCrawling = isCrawling
    # if(_isCrawling is False):
    #     asyncStartCrawl()
    return Response(str(_isCrawling))


@app.route('/fill_empty', methods=['GET'])
def get_fill_empty():
    token = request.args.get('token')
    if(token != 'q1w2e3r4'):
        return Response('Not Found'), 404
    _isCrawling = isCrawling
    # if(_isCrawling is False):
    #     asyncStartFillEmpty()
    return Response(str(_isCrawling))


@app.route('/latest', methods=['GET'])
def get_latest():
    token = request.args.get('token')
    if(token != 'q1w2e3r4'):
        return Response('Not Found'), 404
    _isCrawling = isCrawling
    # if(_isCrawling is False):
    #     asyncStartCrawlLatest()
    return Response(str(_isCrawling))


@app.route('/is_running', methods=['GET'])
def get_test():
    return Response(str(isCrawling))


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=False, port=PORT)
