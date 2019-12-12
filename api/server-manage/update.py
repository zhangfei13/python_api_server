#!/bin/python3

########################################################################################################################
# 功能：api服务自升级
# 说明：自升级只支持post请求
#
########################################################################################################################

import tools


def run(request, param):
    # 判断请求类型
    if request.method != "POST":
        return tools.response(-1001, "please check!")
    # 解析参数
    if param is None or param == "" or len(param) == 0:
        return tools.response(-1, "param is empty!")
    code, subresponse = update(**param)

    return tools.response(code, subresponse)


def update(**param):
    type = param["type"] if dict(param).keys().__contains__("type") else None                                # 升级类型
    pkgUrl = param["pkgUrl"] if dict(param).keys().__contains__("pkgUrl") else None                          # 升级包地址
    if type is None or type == "":
        return -1002, "missing required parameter! [type]"
    if not str(type).startswith("U"):
        return -1002, "type not surport! [%s]" % type
    if pkgUrl is None or pkgUrl == "":
        return -1002, "missing required parameter! [pkgUrl]"

    return 1000, ""

