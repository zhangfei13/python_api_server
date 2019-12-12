#!/usr/bin/python3

import xmltodict
import json
import ctypes
import os
import configparser
from AccuradSite import settings
from django.shortcuts import render as rd, HttpResponse
from suds.client import Client
import tools


def response(code, subdesc='', message=''):
    jsonObject = {}
    jsonObject['code'] = code
    jsonObject['desc'] = tools.ERROR_CODE[str(code)] + " " + subdesc
    jsonObject['datas'] = message
    return HttpResponse(json.dumps(jsonObject), content_type="application/json")


def response_html(message=''):
    return HttpResponse(str(message), content_type="text/html;charset=UTF-8")


def render(request, template_name, context=None, content_type=None, status=None, using=None):
    return rd(request, template_name, context, content_type, status, using)


def xml2json(xml, encoding='utf-8'):
    try:
        convertJson = xmltodict.parse(xml, encoding=encoding)
        jsonStr = json.dumps(convertJson, indent=4)
        return jsonStr
    except Exception as e:
        print(str(e))


def json2xml(js, encoding='utf-8'):
    convertXml = ''
    jsDict = json.loads(js)

    try:
        convertXml = xmltodict.unparse(jsDict, encoding=encoding)
    except:
        convertXml = xmltodict.unparse({'request': jsDict}, encoding=encoding)
    finally:
        return convertXml


def storage2Json(Storage):
    ret = []
    if Storage is None:
        return None
    try:
        for k, item in enumerate(Storage):
            obj = {}
            for k, v in enumerate(item):
                obj[v] = item[v]
            ret.append(obj)
    except:
        ret.append(Storage)
    if len(ret) == 0:
        return None
    return ret


def callc(path):
    if not os.path.exists(path):
        return None
    try:
        c_instance = ctypes.cdll.LoadLibrary(path)
    except Exception as e:
        print(str(e))
        c_instance = None
        pass

    return c_instance


def soapClient(addr):
    try:
        client = Client(addr)
    except Exception as e:
        print("create soap clent error!")
        return None

    return client


def utf8ToGbk(utf8):
    unicode = utf8.decode('utf-8')
    return unicode.encode('gbk')


def gbkToUtf8(gbk):
    unicode = gbk.decode('gbk')
    return unicode.encode('utf-8')


def dictreverse(mapping):
    """
    Returns a new dictionary with keys and values swapped.

        >>> dictreverse({1: 2, 3: 4})
        {2: 1, 4: 3}
    """
    iteritems = lambda d: iter(d.items())
    return dict([(value, key) for (key, value) in iteritems(mapping)])


def getDBConf(type):
    conf = configparser.ConfigParser()
    conf.read(os.path.join(settings.BASE_DIR, "conf/conf.ini"))
    dbinfo = {}

    if not conf.has_section(type):
        return False, "[%s] not exests in conf.ini" % str(type)

    if conf.has_option(type, "HOST"):
        dbinfo['host'] = conf.get(type, "HOST")
    else:
        return False, "[HOST] not exests in conf.ini"

    if conf.has_option(type, "PORT"):
        dbinfo['port'] = int(conf.get(type, "PORT"))

    if conf.has_option(type, "DBN"):
        dbinfo['dbn'] = conf.get(type, "DBN")
    else:
        return False, "[DBN] not exests in conf.ini"

    if conf.has_option(type, "DB"):
        dbinfo['db'] = conf.get(type, "DB")
    else:
        return False, "[DB] not exests in conf.ini"

    if conf.has_option(type, "USER"):
        dbinfo['user'] = conf.get(type, "USER")
    else:
        return False, "[USER] not exests in conf.ini"

    if conf.has_option(type, "PW"):
        dbinfo['pw'] = conf.get(type, "PW")
    else:
        return False, "[PW] not exests in conf.ini"

    if dbinfo['dbn'] == 'oracle':
        if conf.has_option(type, "SERVICE"):
            dbinfo['service'] = conf.get(type, "SERVICE")
        else:
            return False, "[SERVICE] not exests in conf.ini"

    return True, dbinfo
