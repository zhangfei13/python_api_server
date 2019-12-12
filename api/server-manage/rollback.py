#!/bin/python3

########################################################################################################################
# 功能：api服务升级回退
# 说明：回退只支持post请求
#
########################################################################################################################

import json
from django.shortcuts import render, HttpResponse
import tools


def run(request, param):
    # 判断请求类型
    if request.method != "POST":
        return tools.response(-1001, "please check!")
    # 解析参数
    if param is None or param == "" or len(param) == 0:
        return tools.response(-1, "param is empty!")
    code, subresponse = rollback(**param)

    return tools.response(code, subresponse)


def rollback(**param):
    type = param["type"] if dict(param).keys().__contains__("type") else None                                # 升级类型
    if type is None or type == "":
        return -1002, "missing required parameter! [type]"
    if not str(type).startswith("R"):
        return -1002, "type not surport! [%s]" % type

    return 1500, ""

