#!/bin/python3

########################################################################################################################
# 功能：连接oracle
# 说明：connect必须携带参数 服务名service，service参数只针对oracle有效
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
        # ############################ 结构化操作数据库 ############################ #
        # 结构化查询select
        entries1 = db.select('"test1"', what="*", where="A='hehe'", order="D desc", limit=3)

        # 结构化单行插入insert
        entries2 = db.insert('"test1"', A="sigal", B="n", C=0, D=10.1)

        # 结构化多行插入multiple_insert
        db.supports_multiple_insert = True
        values = [{"A": "muti1", "B": "p", "C": 6, "D": 11.1}, {"A": "muti2", "B": "q", "C": 6, "D": 11.2},
                  {"A": "muti3", "B": "r", "C": 6, "D": 11.3}]
        entries3 = db.multiple_insert('"test1"', values=values)

        # 结构化更新update
        entries4 = db.update('"test1"', where="A='fad'", B='mn', C=2)

        # 结构化删除delete
        entries5 = db.delete('"test1"', where="A='ferry'")

        # ############################ 非结构化操作数据库, 可以执行复杂操作, 查询语句返回storage结果集, 其他返回影响行数 ############################ #
        sql = """select m.A, m.B, m.C, m.D, n.F, n.G from "test1" m, "test2" n where m.A = n.E """
        entries6 = db.exec(sql)
    except Exception as e:
        print('执行sql failed! [%s]' % str(e))
        settings.LOCK.release()
        raise
    settings.LOCK.release()

    # 结果打包成json
    jsonData = tools.storage2Json(entries1)

    return tools.response(0, '', jsonData)

