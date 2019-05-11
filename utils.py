import requests
import requests_cache

def get_derived_quantity(data, key):
    if key == "eff_lumi":
        nevents = data.get("nevents_out",-1)
        xsec = data.get("xsec",-1)
        kfact = data.get("kfact",-1)
        efact = data.get("efact",-1)
        return round(nevents/(xsec*kfact*efact*1000.),5)
    if key == "scale1fb":
        nevents = data.get("nevents_out",-1)
        xsec = data.get("xsec",-1)
        kfact = data.get("kfact",-1)
        efact = data.get("efact",-1)
        return round((xsec*kfact*efact*1000./nevents),7)
    return None

def pipe_grep(keys,data):
    if type(data) == list:
        out = []
        for i,x in enumerate(data):
            if type(x) is not dict: 
                out.append(x)
                continue
            d = {}
            if len(keys) >= 1:
                for key in keys:
                    out.append({ key:x.get(key,get_derived_quantity(x,key)) for key in keys })
            # If it was just 1 key, then flatten it (remove the dict layer)
            if len(keys) == 1:
                d = d.values()[0]
            out.append(d)
        return out
    elif type(data) == dict:
        if len(keys) > 0:
            out = {}
            for key in keys: out[key] = data.get(key,None)
            return out
    else:
        return data

def pipe_stats(keys,data):
    if type(data) == list:
        out = []
        for elem in data:
            try: out.append(float(elem))
            except: pass
        if len(out) > 0:
            out = {
                    "count": len(out),
                    "sum": sum(out),
                    "min": min(out),
                    "max": max(out),
                    "mean": round(1.0*sum(out)/len(out),5),
                    }
        else:
            out = {"count": len(data)}
        return out
    else:
        return data

def pipe_sort(keys,data):
    if type(data) == list:
        data = sorted(data)
    else:
        return data

def transform_output(payload, pipes):
    # transform output according to piped commands ("verbs")
    # /Gjet*/*/* | grep location,dataset_name,scale1fb | grep scale1fb | stats
    # /Gjet*/*/* | grep dataset_name | sort
    for pipe in pipes:
        parts = pipe.strip().split(None,1)
        if len(parts) == 1:
            verb = parts[0].strip()
        elif len(parts) == 2:
            verb, keys = parts
            keys = map(lambda x: x.strip(), keys.split(","))
        else:
            verb = parts[0]
        if verb == "grep":
            payload = pipe_grep(payload)
        elif verb == "stats":
            payload = pipe_stats(payload)
        elif verb == "sort":
            payload = pipe_sort(payload)
    return payload

def transform_input(query):
    # parse extra information in query if it's not just the dataset
    # /Gjet*/*/*, cms3tag=*07*06* | grep location,dataset_name
    # ^^dataset^^ ^^^selectors^^^   ^^^^^^^^^^^pipes^^^^^^^^^^
    selectors = []
    pipes = []
    entity = None
    if "|" in query:
        first = query.split("|")[0].strip()
        pipes = query.split("|")[1:]

        if "," in first:
            entity = first.split(",")[0].strip()
            selectors = first.split(",")[1:]
        else:
            entity = first
    elif "," in query:
        entity = query.split(",")[0].strip()
        selectors = query.split(",")[1:]
    else:
        entity = query.strip()
    return entity, selectors, pipes

def enable_requests_caching(cachename,expire_after=3600):
    requests_cache.install_cache(cachename,backend="sqlite",expire_after=expire_after,
            allowable_methods=("GET","POST"),
            fast_save=True,)

def pprint(obj,style="monokai"):
    # stolen from https://gist.github.com/EdwardBetts/0814484fdf7bbf808f6f
    from pygments import highlight
    from pygments.lexers import PythonLexer
    from pygments.formatters import Terminal256Formatter
    from pprint import pformat
    print highlight(pformat(obj), PythonLexer(), Terminal256Formatter(style=style))
