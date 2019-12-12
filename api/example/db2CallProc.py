#!/bin/python3

########################################################################################################################
# 功能：DB2存储过程
# 说明：
#
########################################################################################################################

import tools
from AccuradSite import settings


def run(request, param):
    state, dbinfo = tools.getDBConf("DB")
    if not state:
        return tools.response(-1, dbinfo)

    db = tools.database(**dbinfo)
    db.printing = True
    # 执行方式
    settings.LOCK.acquire()
    try:
        # ############################ 存储过程 ############################ #
        parmout = [tools.CURSOR]
        results = db.callproc("select_test1", [], parmout)
        print("parmout0: ", parmout)

        # 返回固定个数值, 返回结果集, 在tools.CURSOR字段中, 可通过parmout[i]获取
        parmout = [tools.INT, tools.CURSOR, tools.FLOAT]
        results = db.callproc("select_test", ["muti1"], parmout)
        print("parmout: ", parmout)

    except Exception as e:
        print('执行sql failed! [%s]' % str(e))
        settings.LOCK.release()
        raise
    settings.LOCK.release()
    print("return: ", results)

    # storage结果打包成json
    jsonData = tools.storage2Json(None)

    return tools.response(0, '', jsonData)

