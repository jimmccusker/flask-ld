from rdflib import *
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
import base64
import random
import datetime
from flask import Flask, request, make_response, render_template, g, session, abort
import sadi

dc = Namespace("http://purl.org/dc/terms/")

def create_sparql_store(endpoint, update_endpoint=None, use_let_syntax=False):
    if update_endpoint == None:
        update_endpoint = endpoint
    store = SPARQLUpdateStore(queryEndpoint=endpoint,
                              update_endpoint=update_endpoint,
                              use_let_syntax=use_let_syntax)
    store.open((endpoint,update_endpoint))
    return store

def create_id():
    return base64.urlsafe_b64encode(bytes(random.random()*datetime.datetime.now().toordinal())).rstrip("=")

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


class LocalResource:
    def __init__(self,cl, store, prefix):
        self.store = store
        self.cl = cl
        self.prefix = prefix

    def create_uri(self):
        ident = create_id()
        return URIRef(self.prefix+ident)

    def create(self, graph):
        uri = self.create_uri()
        inputUri = "#"
        def rebase(triples):
            def replace(x):
                if isinstance(x,URIRef):
                    if x == inputUri:
                        return URIRef(uri)
                    elif x.startswith(inputUri):
                        return URIRef(uri + x)
                return x
            for t in triples:
                yield (replace(t[0]),replace(t[1]),replace(t[2]))
        g = Graph(self.store,URIRef(uri))
        g.remove((None,None,None))
        g += rebase(graph)
        g.add((URIRef(uri),RDF.type,self.cl))
        g.add((URIRef(uri), dc.created, Literal(datetime.datetime.now())))
        g.commit()

        o = Graph()
        o.add((URIRef(inputUri),OWL.sameAs,uri))
        return o        

    def read(self, uri):
        outputGraph = Graph()
        outputGraph += Graph(self.store,uri)
        if len(outputGraph) == 0:
            describe(self.store, uri, outputGraph)
        return outputGraph

    def update(self, uri, graph):
        self.delete(uri)
        g = Graph(self.store, uri)
        g += graph
        g.commit()
        return g

    def delete(self, uri):
        globalgraph = ConjunctiveGraph(self.store)
        globalGraph.remove((uri,None,None))
        g = Graph(store, uri)
        g.remove((None,None,None))
        g.commit()

    def count(self):
        g = ConjunctiveGraph(self.store)
        query = '''select (count(?s) as ?count) where {?s a %s}'''
        return list(g.query(query % self.cl.n3()))[0][0].value

    def list(self):
        query = '''prefix skos: <http://www.w3.org/2004/02/skos/core#>
prefix dc: <http://purl.org/dc/terms/>
prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
prefix owl: <http://www.w3.org/2002/07/owl#>
prefix prov: <http://www.w3.org/ns/prov#>
construct {
    ?s a %s ;
       skos:prefLabel ?prefLabel;
       rdfs:label ?label;
       dc:title ?title;
       dc:identifier ?identifier;
       rdfs:seeAlso ?seeAlso;
       owl:sameAs ?sameAs;
       prov:alternateOf ?altOf;
       prov:specializationOf ?specOf.
} where {
    ?s a %s .
    optional {?s skos:prefLabel ?prefLabel}
    optional {?s rdfs:label ?label}
    optional {?s dc:title ?title}
    optional {?s dc:identifier ?identifier}
    optional {?s rdfs:seeAlso ?seeAlso}
    optional {?s owl:sameAs ?sameAs}
    optional {?s prov:alternateOf ?altOf}
    optional {?s prov:specializationOf ?specOf}
}'''
        g = ConjunctiveGraph(self.store)
        return g.query(query % (self.cl.n3(), self.cl.n3()))

