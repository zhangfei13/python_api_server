#!/bin/python3

########################################################################################################################
# 功能：解析参数
# 说明：http请求参数（get/post)在参数param中，以字典形式存储
#
########################################################################################################################

import tools


def run(request, param):
    # 解析参数
    if param is None or param == "" or len(param) == 0:
        return tools.response(-1, "param is empty!")
    hisId = param["hisId"] if dict(param).keys().__contains__("hisId") else None
    type = param["type"] if dict(param).keys().__contains__("type") else None
    doctorCode = param["doctorCode"] if dict(param).keys().__contains__("doctorCode") else None  # 非必要
    departmentId = param["departmentId"] if dict(param).keys().__contains__("departmentId") else None
    queryType = param["queryType"] if dict(param).keys().__contains__("queryType") else None
    hospitalCode = param["hospitalCode"] if dict(param).keys().__contains__("hospitalCode") else None

    return tools.response(0, '', param)

