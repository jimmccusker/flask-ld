from flask.ext.security.datastore import Datastore, UserDatastore
import rdfalchemy
from rdfalchemy.descriptors import value2object
from rdflib import *
from flask import make_response
import uuid
from copy import copy
from flask.ext import restful
from flaskld.utils import lru
import hashlib

def public(obj):
    return obj.lod_graph, obj.lod_graph

def protected(obj):
    return obj.ld_graph, obj.ld_graph or obj.lod_graph

def private(obj):
    return obj.private_graph, obj.private_graph or obj.ld_graph or obj.lod_graph

void = Namespace("http://rdfs.org/ns/void#")
auth = Namespace("http://vocab.rpi.edu/auth/")
foaf = Namespace("http://xmlns.com/foaf/0.1/")

def load_namespaces(g, l):
    loc = {}
    loc.update(l)
    loc.update(locals())
    for local in loc:
        if isinstance(loc[local],Namespace):
            g.bind(local, loc[local])

class Serializer:
    def __init__(self,format):
        self.format = format
    def __call__(self, graph, code, headers=None):
        resp = make_response(graph.serialize(format=self.format),code)
        resp.headers.extend(headers or {})
        return resp

def JsonldSerializer(graph, code, headers=None):
    context = dict([(x[0],str(x[1])) for x in graph.namespace_manager.namespaces()])
    resp = make_response(graph.serialize(format='json-ld', context=context, indent=4).decode(),code)
    resp.headers.extend(headers or {})
    return resp    

class Api(restful.Api):
    def __init__(self, *args, **kwargs):
        super(Api, self).__init__(*args, **kwargs)
        self.representations = {
            'application/xml': Serializer("xml"),
            "application/rdf+xml":Serializer('xml'),
            "text/rdf":Serializer('xml'),
            'application/x-www-form-urlencoded':Serializer('xml'),
            'text/turtle':Serializer('turtle'),
            'application/x-turtle':Serializer('turtle'),
            'text/html':Serializer('json-ld'),
            'text/plain':Serializer('nt'),
            'text/n3':Serializer('n3'),
#            'text/html': output_html,
            'application/json': JsonldSerializer,
        }

def tag_datastore(fn):
    def f(self,*args,**kw):
        result = fn(self,*args,**kw)
        if result:
            result.datastore = self
        return result
    return f

class RDFAlchemyDatastore(Datastore):

    def __init__(self, db, classes):
        Datastore.__init__(self, db)
        db.datastore = self
        self.classes = classes

    def commit(self):
        self.db.store.commit()

    def put(self, model):
        #self.db.add(model)
        if model.resUri == URIRef("#"):
            print model.db
            g = model.local_api.create(model.db)
            newURI = g.value(URIRef("#"),OWL.sameAs)
            model = model.local_api.alchemy(newURI)
        else:
            model.local_api.update(model.db, model.resUri)
        return model

    def delete(self, model):
        model.local_api.delete(model)

    @lru
    @tag_datastore
    def get(self,resUri):
        for t in self.db.objects(resUri,rdfalchemy.RDF.type):
            if str(t) in self.classes:
                result = self.classes[str(t)](resUri)
                result.datasource = self
                return result
        return None

class RDFAlchemyUserDatastore(RDFAlchemyDatastore, UserDatastore):
    """A SQLAlchemy datastore implementation for Flask-Security that assumes the
    use of the Flask-SQLAlchemy extension.
    """
    def __init__(self, db, classes, user_model, role_model):
        RDFAlchemyDatastore.__init__(self, db, classes)
        UserDatastore.__init__(self, user_model, role_model)

    @lru
    @tag_datastore
    def get_user(self, identifier):
        if isinstance(identifier,rdfalchemy.URIRef):
            return self.user_model(identifier)
        for attr in ['email']:
            rv = None
            try:
                rv = self.user_model.get_by(**{attr:identifier})
            except:
                pass
            if rv is not None:
                import flask_security.utils
                return rv

    def _is_numeric(self, value):
        try:
            int(value)
        except ValueError:
            return False
        return True

    @lru
    @tag_datastore
    def find_user(self, **kwargs):
        if 'id' in kwargs:
            return self.user_model(rdfalchemy.URIRef(kwargs['id']))
        try:
            return self.user_model.get_by(**kwargs)
        except:
            return None

    @lru
    @tag_datastore
    def find_role(self, role,**kwargs):
        if 'id' in kwargs:
            return self.role_model(rdfalchemy.URIRef(kwargs['id']))
        try:
            return self.role_model.get_by(name=role)
        except:
            return None
