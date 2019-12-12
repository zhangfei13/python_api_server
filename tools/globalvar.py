#!/usr/bin/env python
# global var

ERROR_CODE = {
    "0":         "success",
    "-1":        "config error!",
    "-2":        "url error!",
    "-3":        "script format error!",
    "-4":        "",
    "-5":        "",
    "-6":        "",
    "-7":        "",
    "-8":        "",
    "-9":        "",
    "-10":       "call so error!",
    "-11":       "",
    "-12":       "",
    "-13":       "",
    "-15":       "",
    "-16":       "",
    "-17":       "",
    "-18":       "",
    "-19":       "",
    "-20":       "soap error",
    "-21":       "",
    "-22":       "",
    "-23":       "",

    # 用于服务自升级/回退 begin
    "1000":     "update success",
    "-1001":    "request method not surport!",
    "-1002":    "param error!",

    "1500":     "rollback success",
    "-1999":    "",
    # 用于服务自升级/回退 end

    "-9999":     ""
}


INT = 1
FLOAT = 2
STRING = 3
DATE = 4
DATETIME = 5
BINARY = 6
BIT = 7
IMAGE = 8
DECIMAL = 9
CURSOR = 20
