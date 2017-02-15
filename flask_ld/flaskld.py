from rdflib import *
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
import base64
import random
from datetime import datetime
import re
from flask import Flask, request, make_response, render_template, g, session, abort
from flask_admin import BaseView, expose
from flask_admin.actions import ActionsMixin

from flask_ld.utils import slugify

import rdfalchemy
from flask_admin.model import BaseModelView

from form import get_form, get_label

import sadi

dc = Namespace("http://purl.org/dc/terms/")
flaskld = Namespace("http://vocab.rpi.edu/flaskld/")

from jinja2 import Template

def create_sparql_store(endpoint, update_endpoint=None, use_let_syntax=False):
    if update_endpoint == None:
        update_endpoint = endpoint
    store = SPARQLUpdateStore(queryEndpoint=endpoint,
                              update_endpoint=update_endpoint)
    store.open((endpoint,update_endpoint))
    return store

def create_id():
    return base64.urlsafe_b64encode(bytes(random.random()*datetime.now().toordinal())).rstrip("=")

def describe(store, uri, outputGraph):
    query = '''PREFIX hint: <http://www.bigdata.com/queryHints#>
describe %s where { hint:Query hint:describeMode "CBD". }'''
    g = ConjunctiveGraph(store)
    try:
        outputGraph += g.query(query % uri.n3())
    except:
        outputGraph += g.query("construct {%s ?p ?o} where { %s ?p ?o}"% (uri.n3(),uri.n3()))

def _create_binding(value, datatype):
    if datatype == URIRef:
        return URIRef(value)
    elif instanceof(datatype,URIRef):
        return Literal(value, datatype=datatype)
    else:
        return Literal(value)

def sparql_select(fn):
    import rdflib.plugin
    from rdflib.store import Store
    from rdflib.parser import Parser
    from rdflib.serializer import Serializer
    from rdflib.query import ResultParser, ResultSerializer, Processor, Result, UpdateProcessor
    from rdflib.exceptions import Error
    rdflib.plugin.register('sparql', Result,
                           'rdflib.plugins.sparql.processor', 'SPARQLResult')
    rdflib.plugin.register('sparql', Processor,
                           'rdflib.plugins.sparql.processor', 'SPARQLProcessor')
    rdflib.plugin.register('sparql', UpdateProcessor,
                           'rdflib.plugins.sparql.processor', 'SPARQLUpdateProcessor')

    def wrapper(*args, **kwargs):
        db, query, parameters = fn(*args, **kwargs)
        bindings = dict([(name, _create_binding(request.args[name],datatype)) 
                    for name, datatype in parameters.items() 
                    if name in request.args])
        if 'limit' in request.args:
            query += '\nLIMIT %s' % int(request.args['limit'])
        if 'offset' in request.args:
            query += '\nOFFSET %s' % int(request.args['offset'])

        contentType = request.headers['Accept']
        if 'user_id' in session:
            bindings['user'] = URIRef(session['user_id'])
        return sadi.serialize(db.query(query, initBindings=bindings),contentType)
    wrapper.__name__ = fn.__name__
    return wrapper

def rebase(graph, inputUri, uri):
    def replace(x):
        if isinstance(x, Graph):
            x = x.identifier
        if isinstance(x,URIRef):
            if x == inputUri:
                return URIRef(uri)
            elif x.startswith(inputUri):
                #print x, inputUri, uri, x.replace(inputUri,uri)
                return URIRef(x.replace(inputUri,uri))
        return x
    if hasattr(graph, "quads"):
        graph = graph.quads()
    for t in graph:
        yield tuple([replace(x) for x in t])

class LocalResource:
    def __init__(self, cl, prefix, store, vocab, lod_prefix, mixin=object, name=None):
        self.inputClass = cl
        self.store = store
        self.vocab = vocab
        self.lod_prefix = lod_prefix
        self.prefix = prefix
        self.clResource = self.vocab.resource(cl)
        self.view_template = list(self.clResource[flaskld.hasView])
        self.prefix = prefix
        self.service_prefix = self.lod_prefix + '/' + prefix
        if not self.service_prefix.endswith('/'):
            self.service_prefix += '/'
        self.name = name
        if name is None:
            self.name = prefix

        self.alchemy = create_model(self,mixin)

    def add_api(self, api):
        me = self
        class LDResource(LinkedDataResource):
            def __init__(self):
                LinkedDataResource.__init__(self, me)
        self.LDResource = LDResource

        class ListLDResource(LinkedDataList):
            def __init__(self):
                LinkedDataList.__init__(self, me)
        self.ListLDResource = ListLDResource

        api.add_resource(ListLDResource,'/'+self.prefix,endpoint=str(self.prefix+"listldresource"))
        api.add_resource(LDResource, '/'+self.prefix+'/<string:ident>',
                         endpoint=str(self.prefix+"linkeddataresource"))

    def create(self,inputGraph):
        outputGraph = Graph()
        i = URIRef("#")
        ident = create_id()
        if self.clResource.value(flaskld.key):
            ident = slugify(inputGraph.value(i, self.clResource.value(flaskld.key).identifier))
        uri = self.service_prefix+str(ident)
        def rebase(triples):
            def replace(x):
                if isinstance(x,URIRef):
                    if x == i:
                        return URIRef(uri)
                    elif x.startswith(i):
                        return URIRef(uri + x)
                return x
            for t in triples:
                yield (replace(t[0]),replace(t[1]),replace(t[2]))
        idb = Graph(self.store,URIRef(uri))
        idb.remove((None,None,None))
        idb += rebase(inputGraph)
        idb.add((URIRef(uri),dc.identifier,Literal(ident)))
        idb.add((URIRef(uri),RDF.type,self.inputClass))
        if session and 'user_id' in session and session['user_id'] is not None:
            idb.add((URIRef(uri), flaskld.hasOwner, URIRef(session['user_id'])))
        idb.add((URIRef(uri), flaskld.hasDate, Literal(datetime.now().strftime("%Y-%m-%d-%H-%M-%S"))))
        idb.add((URIRef(uri), dc.created, Literal(datetime.now())))
        idb.commit()
        outputGraph.add((i,OWL.sameAs,URIRef(uri)))
        outputGraph += idb
        outputGraph.template = None
        return outputGraph

    def read(self, uri):
        db = ConjunctiveGraph(self.store)
        idb = Graph(self.store,uri) 
        result = Graph(identifier=uri)
        # raise RuntimeError(result)
        result += idb
        describe(db.store, uri, result)
        result.template = None
        if len(self.view_template) > 0:
            result.template = self.view_template[0].value
        return result

    def update(self,inputGraph, uri):
        allGraphs = ConjunctiveGraph(self.store)
        allGraphs.remove((uri,None,None))
        idb = Graph(self.store,uri)
        idb.remove((None,None,None))
        idb += inputGraph
        idb.template = None
        self.store.commit()
        return idb

    def delete(self,uri):
        idb = Graph(self.store,uri)
        if len(idb) == 0:
            abort(404, "Resource does not exist or is not deletable.")
        idb.remove((None,None,None))
        g = ConjunctiveGraph(self.store)
        g.remove((uri,None,None))
        g.remove((None,None,uri))

    def count(self):
        db = ConjunctiveGraph(self.store)
        query = '''select (count(?s) as ?count) where { ?s a %s }''' % self.inputClass.n3()
        result = list(db.query(query))[0][0].value
        return result


    _list_query_template = Template('''
        select ?instance where {
            ?instance a {{inputClass.n3()}};
            {% if sort_column != None %}
              {{sort_column.n3()}} ?sortval;
            {% endif %}
            .
        }
        {% if sort_column != None %} ORDER BY  
          {% if sort_desc %}DESC(?sortval){% else %}?sortval{% endif %}
        {% endif %}
        OFFSET {{offset}}
        {% if limit != None %} LIMIT {{limit}} {% endif %}''')

    def list_resources(self, offset=0, limit=None, sort_column=None, sort_desc=False):
        query = self._list_query_template.render(inputClass=self.inputClass, limit=limit, offset=offset, 
                                                 sort_column=sort_column, sort_desc=sort_desc)
        db = ConjunctiveGraph(self.store)
        results = db.query(query)
        for row in results:
            yield row[0]


    def list(self, offset=0, limit=None, sort_column=None, sort_desc=False):
        db = ConjunctiveGraph(self.store)
        g = Graph()

        for instance in self.list_resources(offset, limit, sort_column, sort_desc):
            describe(instance,db,g)
            idb = Graph(self.store,instance)
            describe(instance,idb,g)
        g.template = None
        return g


_mapper_classes = {}

class rdfAbstract:
    @property
    def range_class(self):
        """
        Return the class that this descriptor is mapped to through the
        range_type
        """
        if self.range_type and self.range_type in _mapper_classes:
            try:
                return _mapper_classes[self.range_type]
            except AttributeError:
                log.warn(
                    "Descriptor %s has range of: %s but not yet mapped" % (
                        self, self.range_type))
                return rdfSubject
        else:
            return rdfSubject

class rdfSingle(rdfAbstract, rdfalchemy.rdfSingle):
    pass

class rdfMultiple(rdfAbstract, rdfalchemy.rdfMultiple):
    pass

def create_model(local_api, mixin=object):
    class Resource(rdfalchemy.rdfSubject, mixin):

        _sortable_columns = {}

        _db = None
        @property 
        def db(self):
            if self._db == None:
                #print self.resUri
                if self.resUri != URIRef("#"):
                    self._db = local_api.read(self.resUri)
                else:
                    self._db = Graph()
            return self._db

        def __init__(self, uri="#", **kwargs):
            self.local_api = local_api
            self.resUri = URIRef(uri)
            mixin.__init__(self)
            self.db
            #print kwargs
            if kwargs:
                self._set_with_dict(kwargs)
            #rdfalchemy.rdfSubject.__init__(self,self.resUri, **kwargs)

        @property
        def id(self):
            return self.resUri

        def __str__(self):
            #print self.resUri, self.local_api
            return get_label(self._db.resource(self.resUri))
        
        @classmethod
        def get_by(cls, **kwargs):
            """Class Method, returns a single instance of the class
            by a single kwarg.  the keyword must be a descriptor of the
            class.
            example:

            .. code-block:: python

            bigBlue = Company.get_by(symbol='IBM')

            :Note:
            the keyword should map to an rdf predicate
            that is of type owl:InverseFunctional"""
            if len(kwargs) != 1:
                raise ValueError("get_by wanted exactly 1 but got  %i args\n" +
                                 "Maybe you wanted filter_by" % (len(kwargs)))
            key, value = kwargs.items()[0]
            if isinstance(value, (URIRef, BNode, Literal)):
                o = value
            else:
                o = Literal(value)
            pred = cls._getdescriptor(key).pred
            g = ConjunctiveGraph(local_api.store)
            uri = g.value(None, pred, o)
            if uri:
                return cls(uri)
            else:
                raise LookupError("%s = %s not found" % (key, value))

        def __eq__(self, other):
            return hasattr(other,'id') and self.id == other.id

    if len(local_api.view_template) > 0:
        Resource.template = local_api.view_template[0].value
    Resource.__name__ = str(local_api.name)
    Resource.type = rdfMultiple(RDF.type,range_type=OWL.Class)
    Resource.rdf_type = local_api.inputClass
    Resource.clResource = local_api.clResource
    Resource._local_api = local_api
    #fields = [local_api.vocab.resource(x) for x, in local_api.vocab.query('''
    #    select ?field where {
    #      ?c rdfs:subClassOf* ?super.
    #      ?super flaskld:hasField ?field
    #    } order by ?field
    #    ''',initNs=dict(flaskld=flaskld), initBindings=dict(c=local_api.clResource.identifier))]
    #print Resource.rdf_type, list(local_api.clResource[flaskld.hasField])
    for field in local_api.clResource[flaskld.hasField]:
        propertyName = field.value(flaskld.fieldName)
        #print Resource.clResource.identifier, field
        if propertyName == None:
            propertyName = local_api.vocab.qname(field.identifier).split(":")[1].replace("-","_")
        else:
            propertyName = propertyName.value
        propRange = field.value(RDFS.range)
        if propRange != None:
            propRange = propRange.identifier
        if field[RDF.type:OWL.FunctionalProperty]:
            fieldDescriptor = rdfSingle(field.identifier,range_type=propRange)
        else:
            fieldDescriptor = rdfMultiple(field.identifier,range_type=propRange)
        setattr(Resource, propertyName,fieldDescriptor)
        if not Resource.clResource[flaskld.hideField:field.identifier]:
            Resource._sortable_columns[propertyName] = field
        #print local_api.inputClass, propertyName, field.identifier, Resource.__name__
    _mapper_classes[local_api.inputClass] = Resource
    return Resource

class ModelView(BaseModelView):

    _default_sort = dc.identifier

    def __init__(self, local_api, default_sort=None, **kwargs):
        self.local_api = local_api
        label = local_api.clResource.value(RDFS.label)
        if default_sort != None:
            self._default_sort = default_sort
        if (label == None):
            label = re.sub(":_-\/"," ",re.sub("^([^:])+:","",
                           local_api.clResource.graph.qname(local_api.clResource.identifier)))
        else:
            label = label.value
        BaseModelView.__init__(self, local_api.alchemy,  name=label, **kwargs)

    def get_pk_value(self, model):
        return model.resUri

    def scaffold_sortable_columns(self):
        return None

    def init_search(self):
        return False

    def create_model(self,form):
        """
            Create model from form.

            :param form:
                Form instance
        """
        try:
            model = self.model()
            form.populate_obj(model)
            self._on_model_change(form, model, True)
            self.local_api.create(model.db)
        except Exception as ex:
            if not self.handle_view_exception(ex):
                raise

            flash(gettext('Failed to create model. %(error)s', error=str(ex)), 'error')
            log.exception('Failed to create model')
            self.local_api.store.rollback()
            return False
        else:
            self.after_model_change(form, model, True)

        return True

    def update_model(self, form, model):
        """
            Update model from form.

            :param form:
                Form instance
            :param model:
                Model instance
        """
        try:
            form.populate_obj(model)
            self._on_model_change(form, model, False)
            self.local_api.update(model.db, model.resUri)
        except Exception as ex:
            if not self.handle_view_exception(ex):
                raise

            flash(gettext('Failed to update model. %(error)s', error=str(ex)), 'error')
            log.exception('Failed to update model')
            self.local_api.store.rollback()

            return False
        else:
            self.after_model_change(form, model, False)

        return True

    def delete_model(self, model):
        """
            Delete model.

            :param model:
                Model to delete
        """
        try:
            self.on_model_delete(model)
            self.local_api.delete(model.resUri)
            return True
        except Exception as ex:
            if not self.handle_view_exception(ex):
                raise

            flash(gettext('Failed to delete model. %(error)s', error=str(ex)), 'error')
            log.exception('Failed to delete model')
            self.local_api.store.rollback()
            return False

    def get_one(self, id):
        """
            Return a single model by its id.

            :param id:
                Model id
        """
        return self.local_api.alchemy(URIRef(id))

    def get_count(self):
        return self.local_api.count()
    
    def scaffold_form(self):
        return get_form(self.local_api.alchemy)

    def scaffold_list_columns(self):
        columns = self.local_api.alchemy._sortable_columns.keys()
        return columns

    def scaffold_sortable_columns(self):
        return self.local_api.alchemy._sortable_columns.keys() 

    def _get_default_order(self):
        return self._default_sort

    
    def get_list(self, page, sort_column, sort_desc, search, filters, execute=True):
        """
            Return models from the database.

            :param page:
                Page number
            :param sort_column:
                Sort column name
            :param sort_desc:
                Descending or ascending sort
            :param search:
                Search query
            :param execute:
                Execute query immediately? Default is `True`
            :param filters:
                List of filter tuples
        """

        # Will contain names of joined tables to avoid duplicate joins
        joins = set()

        count = self.get_count()

        limit = self.page_size
        offset = self.page_size * page

        # Sorting
        if sort_column is not None:
            if sort_column in self.local_api.alchemy._sortable_columns:
                sort_field = self.local_api.alchemy._sortable_columns[sort_column].identifier
        else:
            sort_field = self._get_default_order()

        instances = self.local_api.list_resources(offset, limit, sort_field, sort_desc)

        def gen():
            for i in instances:
                yield self.get_one(i)

        return count, gen()

    def handle_view_exception(self,ex):
        #print ex
        raise ex

    
