#!/bin/python3

########################################################################################################################
# 功能：创建soap客户端, 调用soap服务
# 说明：
#
########################################################################################################################

import tools


def run(request, param):
    addr = 'http://ws.webxml.com.cn/WebServices/MobileCodeWS.asmx?wsdl'
    client = tools.soapClient(addr)
    if client is None:
        return tools.response(-20, 'create soap client error!')
    info = client.service.getMobileCodeInfo('15029228634', '')
    print(info)

    return tools.response(0, '', info)

