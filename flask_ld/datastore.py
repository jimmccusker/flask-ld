from flask.ext.security.datastore import Datastore, UserDatastore
import rdfalchemy
from rdfalchemy.descriptors import value2object
from rdflib import *
from flask import make_response
import uuid
from copy import copy
from flask.ext import restful
from utils import lru
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
    #print g.namespace_manager
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
    #print context
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


class rdfSingle(rdfalchemy.rdfSingle):

    '''This is a Descriptor
    Takes a the URI of the predicate at initialization
    Expects to return a single item
    on Assignment will set that value to the
    ONLY triple with that subject,predicate pair'''
    def __init__(self, pred, cacheName=None, range_type=None, graph=private):
        super(rdfSingle, self).__init__(pred, cacheName, range_type)
        self.graph = graph

    def __get__(self, obj, cls):
        if obj is None:
            return self
        if self.name in obj.__dict__:
            return obj.__dict__[self.name]
        val = self.graph(obj)[1].value(obj.resUri, self.pred)
        if isinstance(val, Literal):
            val = val.toPython()
        elif isinstance(val, (BNode, URIRef)):
            val = obj.datastore.get(val)
        obj.__dict__[self.name] = val
        return val

    def __set__(self, obj, value):
        # setattr(obj, self.name, value)  #this recurses indefinatly
        if isinstance(value, (list, tuple, set)):
            raise AttributeError(
                "to set an rdfSingle you must pass in a single value")
        obj.__dict__[self.name] = value
        o = value2object(value)
        self.graph(obj)[0].set((obj.resUri, self.pred, o))

class rdfMultiple(rdfalchemy.rdfMultiple):

    '''This is a Descriptor
       Expects to return a list of values (could be a list of one)'''
    def __init__(self, pred, cacheName=None, range_type=None, graph=private):
        super(rdfMultiple, self).__init__(pred, cacheName, range_type)
        self.graph = graph

    def __get__(self, obj, cls):
        if obj is None:
            return self
        db = self.graph(obj)[1]
        if self.name in obj.__dict__:
            return obj.__dict__[self.name]
        val = [o for o in db.objects(obj.resUri, self.pred)]
        #print obj.resUri, self.pred, val
        # check to see if this is a Container or Collection
        # if so, return collection as a list
        if (len(val) == 1
            ) and (
                not isinstance(val[0], Literal)
            ) and (
                db.value(val[0], RDF.first
                             ) ):
            val = getList(obj, self.pred)
        val = [(obj.datastore.get(v) if isinstance(v, (BNode, URIRef))
                else v.toPython())
               for v in val]
        #obj.__dict__[self.name] = val
        return val

    def __set__(self, obj, newvals):
        if not isinstance(newvals, (list, tuple)):
            raise AttributeError(
                "to set a rdfMultiple you must pass in " +
                "a list (it can be a list of one)")
        try:
            oldvals = obj.__dict__[self.name]
        except KeyError:
            oldvals = []
            obj.__dict__[self.name] = oldvals
        db = self.graph(obj)[0]
        for value in oldvals:
            if value and not value in newvals:
                db.remove((obj.resUri, self.pred, value2object(value)))
        for value in newvals:
            if value not in oldvals:
                db.add((obj.resUri, self.pred, value2object(value)))
        obj.__dict__[self.name] = copy(newvals)


class Resource(rdfalchemy.rdfSubject):
    uri_template = None
    lod_graph_template = None
    ld_graph_template = None
    graph_template = None
    datastore = None
    private_graph = None
    ld_graph = None
    lod_graph = None

    @property
    def id(self):
        return self.resUri
    
    def _setup_from_graph_templates(self, **kwargs):
        if self.graph_template:
            self.private_graph = Graph(self.base_db.store,
                                       rdfalchemy.URIRef(self.graph_template.format(**kwargs)),
                                       self.base_db.namespace_manager)
            #self.db = self.private_graph
        elif not self.private_graph:
            self.private_graph = self.base_db
        if self.ld_graph_template:
            self.ld_graph = Graph(self.base_db.store,
                                  rdfalchemy.URIRef(self.ld_graph_template.format(**kwargs)),
                                  self.base_db.namespace_manager)
            #self.db = self.ld_graph
        elif not self.ld_graph:
            self.ld_graph = self.private_graph
        if self.lod_graph_template:
            self.lod_graph = Graph(self.base_db.store,
                                   rdfalchemy.URIRef(self.lod_graph_template.format(**kwargs)),
                                   self.base_db.namespace_manager)
            #self.db = self.lod_graph
        elif not self.lod_graph:
            self.lod_graph = self.ld_graph

    def _introspect_graphs(self, resUri):
        private_uri = self.base_db.value(resUri,auth.inPrivateDataset)
        if private_uri:
            self.private_graph = Graph(self.base_db.store,private_uri,
                                       self.base_db.namespace_manager)
            #self.db = self.private_graph
        else:
            self.private_graph = self.base_db
        ld_uri = self.base_db.value(resUri,auth.inLDDataset)
        if ld_uri:
            self.ld_graph = Graph(self.base_db.store,ld_uri,
                                  self.base_db.namespace_manager)
            #self.db = self.ld_graph
        else:
            self.ld_graph = self.private_graph
        lod_uri = self.base_db.value(resUri,void.inDataset)
        if lod_uri:
            self.lod_graph = Graph(self.base_db.store,lod_uri,
                                   self.base_db.namespace_manager)
            #self.db = self.lod_graph
        else:
            self.lod_graph = self.ld_graph

    def __init__(self, resUri=None, id=None, **kwargs):
        self.base_db = self.db

        if resUri == None and id != None:
            resUri = rdfalchemy.URIRef(id)
        
        if len(kwargs) > 0:
            self._setup_from_graph_templates(**kwargs)
        else:
            self._introspect_graphs(resUri)
        if resUri == None and self.uri_template != None:
            resUri = rdfalchemy.URIRef(self.uri_template.format(**kwargs))
        rdfalchemy.rdfSubject.__init__(self,resUri, **kwargs)
        if len(kwargs) > 0:
            if self.rdf_type and not list(public(self)[0].triples(
                (self.resUri, RDF.type, self.rdf_type))):
                public(self)[0].add((self.resUri, RDF.type, self.rdf_type))
            self.private_dataset = NamedGraph(self.private_graph.identifier)
            self.ld_dataset = NamedGraph(self.ld_graph.identifier)
            self.lod_dataset = NamedGraph(self.lod_graph.identifier)
            self.seeAlso = [NamedGraph(self.ld_graph.identifier)]
    
    lod_dataset = rdfSingle(void.inDataset, void.Dataset, graph=public)
    ld_dataset = rdfSingle(auth.inLDDataset, void.Dataset, graph=protected)
    private_dataset = rdfSingle(auth.inPrivateDataset, void.Dataset, graph=private)
    seeAlso = rdfMultiple(RDFS.seeAlso, graph=public)

    @property
    def id(self):
        return self.resUri

class NamedGraph(Resource):
    rdf_type = void.Dataset
    def __init__(self, resUri, **kwargs):
        Resource.__init__(self, resUri, **kwargs)
        self.lod_graph = self.ld_graph = self.private_graph = Graph(self.db.store,resUri,
            self.db.namespace_manager)
        self.db = self.lod_graph

    primary_topic = rdfSingle(foaf.primaryTopic,graph=public)

def tag_datastore(fn):
    def f(self,*args,**kw):
        result = fn(self,*args,**kw)
        if result:
            #print self, result
            result.datastore = self
        return result
    return f

class RDFAlchemyDatastore(Datastore):

    def __init__(self, db, classes):
        Datastore.__init__(self, db)
        db.datastore = self
        self.classes = classes

    def commit(self):
        self.db.commit()

    @tag_datastore
    def put(self, model):
        #self.db.add(model)
        if model.resUri == URIRef("#"):
            #print model.db
            g = model.local_api.create(model.db)
            newURI = g.value(URIRef("#"),OWL.sameAs)
            model = model.local_api.alchemy(newURI)
        else:
            model.local_api.update(model.db, model.resUri)
        return model

    def delete(self, model):
        self.db.remove(model)

    @lru
    @tag_datastore
    def get(self,resUri):
        #print resUri, 'a', [x for x in self.db.objects(resUri,rdfalchemy.RDF.type)]
        for t in self.db.objects(resUri,rdfalchemy.RDF.type):
            if str(t) in self.classes:
                result = self.classes[str(t)](resUri)
                result.datasource = self
                return result
        return Resource(resUri)

class RDFAlchemyUserDatastore(RDFAlchemyDatastore, UserDatastore):
    """A SQLAlchemy datastore implementation for Flask-Security that assumes the
    use of the Flask-SQLAlchemy extension.
    """
    def __init__(self, db, classes, user_model, role_model):
        RDFAlchemyDatastore.__init__(self, db, classes)
        UserDatastore.__init__(self, user_model, role_model)

    @tag_datastore
    def get_user(self, identifier):
        if isinstance(identifier,rdfalchemy.URIRef):
            return self.user_model.query.get(identifier)
        for attr in ['name','email','identifier']:
            rv = None
            try:
                rv = self.user_model.get_by(**{attr:identifier})
            except:
                pass
            if rv is not None:
                return rv

    def _is_numeric(self, value):
        try:
            int(value)
        except ValueError:
            return False
        return True

    @tag_datastore
    def find_user(self, **kwargs):
        #print kwargs
        if 'id' in kwargs:
            return self.user_model(uri=rdfalchemy.URIRef(kwargs['id']))
        try:
            return self.user_model.get_by(**kwargs)
        except:
            return None

    @tag_datastore
    def find_role(self, role, **kwargs):
        #print kwargs
        if 'id' in kwargs:
            return self.role_model(rdfalchemy.URIRef(kwargs['id']))
        try:
            return self.role_model.get_by(name=role)
        except:
            return None
