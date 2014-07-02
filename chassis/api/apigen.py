# Standard Library
import logging

# Third Party
import requests
import mixingboard

# Local


class API(object):

    def __init__(self, mixingboardName, app):

        self.mixingboardName = mixingboardName
        self.app = app

        self.baseUrl = None
        mixingboard.discoverService(mixingboardName, self.setBaseUrl) 

        for rule in app.url_map.iter_rules():

            def generateRuleFn(r):

                methods = r.methods
                method = None
                if "GET" in methods:
                    method = "get"
                if "POST" in methods:
                    method = "post"
                if "PUT" in methods:
                    method = "put"
                if "DELETE" in methods:
                    method = "delete"
                
                def ruleFn(*args, **kwargs):

                    params = {}
                    data = {}
                    if method == "get":
                        params = kwargs
                    else:
                        data = kwargs

                    rule = r.rule
                    for arg in args:
                        rule = re.sub('<[^>]*>', str(arg), rule, count=1)

                    url = self.baseUrl+rule

                    res = getattr(requests, method)(url, params=params, data=data)
            
                    try:
                        return res.json(), res.status_code
                    except:
                        return res.text, res.status_code

                return ruleFn

            setattr(self, rule.endpoint, generateRuleFn(rule))
    
    def setBaseUrl(self, servers):
        server = servers[0]
        self.server = server
        self.baseUrl = "http://%s:%s" % (server["host"], server["port"])

        logging.info("GOT '%s' SERVICE: %s" % (
            self.mixingboardName,
            self.baseUrl
        ))
