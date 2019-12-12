#!/bin/python3
import os
import tools
from AccuradSite import settings

########################################################################################################################
# 功能：调用c/c++动态库
# 说明：tools.callc获取动态库实例, 然后用实例调用库中函数
#
########################################################################################################################


def run(request, param):
    try:
        # ############################ 存储过程 ############################ #
        path = os.path.join(settings.BASE_DIR, "so/libmax.so")
        if not os.path.exists(path):
            return tools.response(-10, 'path not exist[%s]' % path)

        instance = tools.callc(path)
        if instance is None:
            return tools.response(-10, 'get instance failed!')
        max = instance.max(3, 4)

    except Exception as e:
        print('调用c so failed! [%s]' % str(e))
        return tools.response(-10, str(e))

    # storage结果打包成json
    jsonData = tools.storage2Json(max)

    return tools.response(0, '', jsonData)
