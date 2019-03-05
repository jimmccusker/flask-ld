from __future__ import print_function
from rdflib import *
from flask_ld.flaskld import LocalResource
from flask import Flask, request, make_response, render_template, g, session, abort
from flask_restful import Resource, Api
import sadi

class LinkedDataResourceList(Resource):
    def __init__(self, local_resource):
        self.local_resource = local_resource

    def post(self):
        inputGraph = Graph()
        contentType = request.headers['Content-Type']
        sadi.deserialize(inputGraph,unicode(request.data),contentType)
        outputGraph = self.local_resource.create(inputGraph)
        return outputGraph, 201

    def get(self):
        return self.local_resource.list()

class LinkedDataResource(Resource):
    def __init__(self, local_resource):
        self.local_resource = local_resource

    def _get_uri(self,ident):
        return URIRef(self.local_resource.prefix + ident)

    def get(self,*args,**kwargs):
        uri = self._get_uri(*args,**kwargs)
        result = self.local_resource.read(uri)
        return result

    def delete(self,*args,**kwargs):
        uri = self._get_uri(*args,**kwargs)
        self.local_resource.delete(uri)
        return None, 204

    def put(self,*args,**kwargs):
        uri = self._get_uri(*args,**kwargs)
        inputGraph = Graph()
        contentType = request.headers['Content-Type']
        sadi.deserialize(inputGraph,request.data,contentType)
        result = self.local_resource.update(inputGraph, uri)
        return result, 201

    def post(self,*args,**kwargs):
        uri = self._get_uri(*args,**kwargs)
        inputGraph = Graph()
        contentType = request.headers['Content-Type']
        sadi.deserialize(inputGraph,request.data,contentType)
        result = self.local_resource.update(inputGraph, uri)
        return result, 201

def serializer(mimetype):
    def wrapper(graph, code, headers=None):
        data = ''
        print(graph)
        if graph is not None and hasattr(graph, "serialize"):
            data = graph.serialize(format=sadi.contentTypes[mimetype].outputFormat)
        #print data, code, len(graph), mimetype
        resp = make_response(data, code)
        resp.headers.extend(headers or {})
        print(data)
        return resp
    return wrapper

def rendertemplate(data, code, headers=None):
    headers = headers or {}
    if isinstance(data,rdfalchemy.rdfSubject):
        uri = data.resUri
    else:
        uri = data.identifier
    if data.template:
        data = render_template(data.template,uri=uri,g=g,graph=data,ns=ns)
        headers['Content-Type'] = "text/html"
    else:
        data = data.serialize(format="turtle")
        headers['Content-Type'] = "text/turtle"

    resp = make_response(data, code)
    resp.headers.extend(headers or {})
    return resp

class JsonLDSerializer(sadi.DefaultSerializer):
    context = None
    def serialize(self,graph):
        if self.context != None:
            self.bindPrefixes(graph)
            return graph.serialize(format=self.outputFormat,
                                   context= self.context,encoding='utf-8')



sadi.contentTypes['application/json'] = JsonLDSerializer("json-ld")
sadi.contentTypes['application/ld+json'] = JsonLDSerializer("json-ld")
class LinkedDataApi(Api):

    _local_resources = {}

    def __init__(self, app, api_prefix, store, host_prefix, decorators=[]):
        Api.__init__(self, app, prefix=api_prefix)
        self.store = store
        for mimetype in sadi.contentTypes.keys():
            if mimetype is not None:
                self.representations[mimetype] = serializer(mimetype)
        self.representations['text/html'] = rendertemplate

        self.lod_prefix = host_prefix + api_prefix
        self._decorators = decorators

    def __getitem__(self,cl):
        return self._local_resources[cl]

    def create(self, cl, prefix):
        resource = LocalResource(cl, self.store, self.lod_prefix+ "/" + prefix + "/")
        self._local_resources[cl] = resource

        class LDResource(LinkedDataResource):
            decorators = self._decorators
            def __init__(self):
                LinkedDataResource.__init__(self, resource)

        class ListLDResource(LinkedDataResourceList):
            decorators = self._decorators
            def __init__(self):
                LinkedDataResourceList.__init__(self, resource)

        self.add_resource(ListLDResource,'/'+prefix,endpoint=str(prefix+"list"))
        self.add_resource(LDResource, '/'+prefix+'/<string:ident>',
                         endpoint=str(prefix+"ld"))
