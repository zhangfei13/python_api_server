#!/bin/python3

########################################################################################################################
# 功能：sqlserver存储过程
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
        cursor = db._db_cursor()
        # ############################ 存储过程 ############################ #
        declare = "declare @p1 INT declare @p2 DECIMAL(10,2) declare @ret INT"
        exec = "exec @ret = select_test2 'fad',@p1 output,@p2 output"
        select = "select @p1,@p2,@ret"
        cursor.execute(f"%s %s %s" % (declare, exec, select))
        result = cursor.fetchall()  # 得到结果集
        for i in result:
            print(i)
        while cursor.nextset():
            result = cursor.fetchall()
            for i in result:
                print(i)

    except Exception as e:
        print('执行sql failed! [%s]' % str(e))
        settings.LOCK.release()
        raise
    settings.LOCK.release()

    # storage结果打包成json
    jsonData = tools.storage2Json(None)

    return tools.response(0, '', jsonData)

