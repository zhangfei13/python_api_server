#!/bin/python3

########################################################################################################################
# 功能：连接mysql数据库
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
        # ############################ 结构化操作数据库 ############################ #
        # 结构化查询select
        entries1 = db.select("test", what="*", where="a='hehe'", order="d desc", limit=3)

        # 结构化单行插入insert
        entries2 = db.insert("test", a="sigal", b="n", c=0, d=10.1)

        # 结构化多行插入multiple_insert
        db.supports_multiple_insert = True
        values = [{"a": "muti1", "b": "p", "c": 6, "d": 11.1}, {"a": "muti2", "b": "q", "c": 6, "d": 11.2}, {"a": "muti3", "b": "r", "c": 6, "d": 11.3}]
        entries3 = db.multiple_insert("test", values=values)

        # 结构化更新update
        entries4 = db.update("test", where="a='fad'", b='mn', c=2)

        # 结构化删除delete
        entries5 = db.delete("test", where="a='ferry'")

        # ############################ 非结构化操作数据库, 可以执行复杂操作, 查询语句返回storage结果集, 其他返回影响行数 ############################ #
        sql = """select m.a, m.b, m.c, m.d, n.f, n.g from test m, test2 n where m.a = n.e"""
        sql = """insert into test(a,b,c,d) values('ssinsert', 'h', '1', '2')"""
        entries6 = db.exec(sql)
    except Exception as e:
        print('执行sql failed! [%s]' % str(e))
        settings.LOCK.release()
        raise
    settings.LOCK.release()

    # storage结果打包成json
    jsonData = tools.storage2Json(entries1)

    return tools.response(0, '', jsonData)

