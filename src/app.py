from flask import Flask, Response, request, json
import requests
import os
from os import path
import asyncio
from crawler import GrapPage
from threading import Thread
import time
from helper import _print
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


def asyncStartCrawl(quest_type):
    time.sleep(1)
    _print('asyncStartCrawl')
    target = startCrawl
    if (str(quest_type) == '1'):
        target = startFillInfo
    thr = Thread(target=target)
    thr.start()


@app.route('/', methods=['GET'])
def get_():
    return Response('Hola')


@app.route('/start', methods=['GET'])
def get_start():
    token = request.args.get('token')
    quest_type = request.args.get('quest')
    _print(quest_type)
    if(token != 'q1w2e3r4'):
        return Response('Not Found'), 404
    _isCrawling = isCrawling
    if(_isCrawling is False):
        asyncStartCrawl(quest_type)
    return Response(str(_isCrawling))


@app.route('/is_running', methods=['GET'])
def get_test():
    return Response(str(isCrawling))


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=False, port=PORT)
