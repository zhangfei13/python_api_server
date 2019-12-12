from AccuradSite import settings
import importlib
import os
import json
import tools


# Create your views here.
def interface(request):
    name = request.path.lstrip("/")
    if name is None or name == "":
        return tools.response(-2, "the sub url is not gived! [%s]" % name)
    apiPath = os.path.join(settings.BASE_DIR, "api", name + ".py")
    print(apiPath)
    if not os.path.exists(apiPath) or os.path.isdir(apiPath):
        if not os.path.exists(apiPath + "c") or os.path.isdir(apiPath + "c"):
            return tools.response(-2, "the sub url is not give resolve! [%s]" % name)
    name = "api." + str(name).lstrip("/").replace("/", ".")
    lib = importlib.import_module(name)
    if not hasattr(lib, 'run'):
        return tools.response(-3, "there's no entry 'run' in [%s]" % name)

    parms = {}
    if request.method == "GET":
        for key in request.GET.keys():
            parms[key] = request.GET.get(key)
    else:
        parms = request.body.decode("utf-8")

    if parms == "" or parms is None or len(parms) == 0:
        parms = None
    elif request.method == "POST":
        parms = json.loads(parms)
    if parms == "" or parms is None or len(parms) == 0:
        parms = None

    return lib.run(request, parms)



