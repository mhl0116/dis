#!/usr/bin/python

import cgi

def inputToDict(form):
    d = {}
    for k in form.keys():
        d[k] = form[k].value
    return d

form = cgi.FieldStorage()


import api
inp = inputToDict(form)

print "Content-type: application/json"
print "Cache-Control: max-age=300"
print "Access-Control-Allow-Origin: *\n\n"
print api.handle_query(inp)
