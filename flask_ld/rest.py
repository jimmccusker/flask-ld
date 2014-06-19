from rdflib import *
from flask_ld.flaskld import LocalResource
from flask import Flask, request, make_response, render_template, g, session, abort
from flask.ext.restful import Resource, Api
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

    def get(self,ident):
        uri = self._get_uri(ident)
        result = self.local_resource.read(uri)
        return result

    def delete(self,ident):
        uri = self._get_uri(ident)
        self.local_resource.delete(uri)
        return '', 204

    def put(self,ident):
        uri = self._get_uri(ident)
        inputGraph = Graph()
        contentType = request.headers['Content-Type']
        sadi.deserialize(inputGraph,request.data,contentType)
        self.local_resource.update(inputGraph, uri)
        return '', 201

    def post(self,ident):
        return '', 404

def serializer(mimetype):
    def wrapper(graph, code, headers=None):
        data = sadi.serialize(graph,mimetype)
        resp = make_response(data, code)
        resp.headers.extend(headers or {})
        return resp
    return wrapper

class JsonLDSerializer(sadi.DefaultSerializer):
    context = None
    def serialize(self,graph):
        if context != None:
            self.bindPrefixes(graph)
            return graph.serialize(format=self.outputFormat,
                                   context= self.context,encoding='utf-8')


sadi.contentTypes['application/json'] = JsonLDSerializer("json-ld")
class LinkedDataApi(Api):

    _local_resources = {}

    def __init__(self, app, api_prefix, store, host_prefix, decorators=[]):
        Api.__init__(self, app, prefix=api_prefix)
        self.store = store
        for mimetype in sadi.contentTypes.keys():
            self.representations[mimetype] = serializer(mimetype)
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
