from flask.ext.security.datastore import Datastore, UserDatastore
from rdflib import *
from flask import make_response
import uuid
from copy import copy
from flask.ext import restful
from flaskld.utils import lru
import hashlib

void = Namespace("http://rdfs.org/ns/void#")
auth = Namespace("http://vocab.rpi.edu/auth/")
foaf = Namespace("http://xmlns.com/foaf/0.1/")
prov  = Namespace("http://www.w3.org/ns/prov#")

def load_namespaces(g, l):
    loc = {}
    loc.update(l)
    loc.update(locals())
    for local in loc:
        if isinstance(loc[local],Namespace):
            g.bind(local, loc[local])

def tag_datastore(fn):
    def f(self,*args,**kw):
        result = fn(self,*args,**kw)
        if result:
            result.datastore = self
        return result
    return f


# Borrowed and adapted from RDFAlchemy. This is the best bit.

def value2object(value):
    """suitable for a triple takes a value and returns a Literal, URIRef or BNode
    suitable for a triple"""
    if isinstance(value, Resource):
        return value.identifier
    elif isinstance(value, Identifier):
        return value
    else:
        return Literal(value)
    
class rdfAbstract(object):
    """Abstract base class for descriptors
    Descriptors are to map class instance variables to predicates
    optional cacheName is where to store items
    range_type is the rdf:type of the range of this predicate"""
    def __init__(self, pred, cacheName=None, range_type=None):
        self.pred = pred
        self.name = cacheName or pred
        self.range_type = range_type

    def __delete__(self, obj):
        """deletes or removes from the database triples with:
          obj.resUri as subject and self.pred as predicate
          if the object of that triple is a Literal that stop
          if the object of that triple is a BNode
          then cascade the delete if that BNode has no further references to it
          i.e. it is not the object in any other triples.
        """
        # be done ala getList above
        log.debug("DELETE with descriptor for %s on %s", self.pred, obj.n3())
        # first drop the cached value
        if obj.__dict__.has_key(self.name):
            del obj.__dict__[self.name]
        # next, drop the triples
        obj.remove(self.pred)




class rdfSingle(rdfAbstract):
    '''This is a Descriptor
    Takes a the URI of the predicate at initialization
    Expects to return a single item
    on Assignment will set that value to the
    ONLY triple with that subject,predicate pair'''
    def __init__(self, pred, cacheName=None, range_type=None):
        super(rdfSingle, self).__init__(pred, cacheName, range_type)

    def __get__(self, obj, cls):
        if obj is None:
            return self
        if self.name in obj.__dict__:
            return obj.__dict__[self.name]
        log.debug("Geting with descriptor %s for %s", self.pred, obj.n3())
        val = obj.value(__getitem__(self.pred)
        obj.__dict__[self.name]= val
        return val

    def __set__(self, obj, value):
        if isinstance(value,(list,tuple,set)):
            raise AttributeError("to set an rdfSingle you must pass in a single value")
        obj.__dict__[self.name] = value
        o = value2object(value)
        obj.set(self.pred, o)


class rdfMultiple(rdfAbstract):
    '''This is a Descriptor
       Expects to return a list of values (could be a list of one)'''
    def __init__(self, pred, cacheName=None, range_type=None):
        super(rdfMultiple, self).__init__(pred, cacheName, range_type)

    def __get__(self, obj, cls):
        if obj is None:
            return self
        if self.name in obj.__dict__:
            return obj.__dict__[self.name]
        val = list(obj.objects(self.pred))
        log.debug("Geting with descriptor %s for %s", self.pred, obj.n3())
        # check to see if this is a Container or Collection
        # if so, return collection as a list
        if len(val) == 1 \
           and (obj.graph.value(o, RDF.first) or obj.graph.value(o, RDF._1)):
            val = collection.Collection(obj, self.pred)
        val = [(isinstance(v, (BNode,URIRef)) and self.range_class(v) or v.toPython()) for v in val]
        obj.__dict__[self.name] = val
        return val

    def __set__(self, obj, newvals):
        log.debug("SET with descriptor value %s of type %s", newvals, type(newvals))
        if not isinstance(newvals, (list, tuple)):
            raise AttributeError("to set a rdfMultiple you must pass in a list (it can be a list of one)")
        try:
            oldvals = obj.__dict__[self.name]
        except KeyError:
            oldvals = []
            obj.__dict__[self.name] = oldvals
        for value in oldvals:
            if value not in newvals:
                obj.db.remove((obj.resUri, self.pred, value2object(value)))
                log.debug("removing: %s, %s, %s", obj.n3(), self.pred, value)
        for value in newvals:
            if value not in oldvals:
                obj.db.add((obj.resUri, self.pred, value2object(value)))
                log.debug("adding: %s, %s, %s", obj.n3(), self.pred, value)
        obj.__dict__[self.name] = copy(newvals)


class Role(Resource, RoleMixin):
    rdf_type = prov.Role
    name = rdfSingle(rdfalchemy.RDFS.label)
    identifier = rdfSingle(dc.identifier)
    description = rdfSingle(dc.description)

class User(Resource, UserMixin):
    rdf_type = prov.Agent
    name = rdfSingle(foaf.name)
    email = rdfSingle(auth.email)
    identifier = rdfSingle(dc.identifier)
    password = rdfSingle(auth.passwd)
    active = rdfSingle(auth.active)
    confirmed_at = rdfSingle(auth.confirmed, range_type=xsd.datetime)
    roles = rdfMultiple(auth.hasRole, range_type=prov.Role)
    last_login_at = rdfSingle(auth.hadLastLogin, range_type=xsd.datetime)
    current_login_at = rdfSingle(auth.hadCurrentLogin, range_type=xsd.datetime)

    def __init__(self,resUri=None, **kwargs):
        self.datastore = user_datastore
        if resUri == None and 'identifier' not in kwargs:
            kwargs['identifier'] = str(uuid4())

        Resource.__init__(self, resUri, **kwargs)
        print "LOD Graph:", type(self.lod_dataset)
        if resUri == None and self.lod_dataset.primary_topic == None:
            self.lod_dataset.primary_topic = self
        if resUri == None and self.signature_request_service == None:
            self.signature_request_service = URIRef(app.config['lod_prefix']+"request")

        print self, resUri, kwargs

class LinkedDatastore(Datastore):

    def __init__(self, ldapi):
        Datastore.__init__(self, ldapi.store)
        self.ldapi = ldapi

    def commit(self):
        self.ldapi.store.commit()

class LinkedUserDatastore(LinkedDatastore, UserDatastore):
    """An Flask-LD datastore implementation for Flask-Security that assumes the
    use of the Flask-SQLAlchemy extension.
    """
    def __init__(self, db, classes, user_model, role_model):
        LinkedDatastore.__init__(self, db, classes)
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
