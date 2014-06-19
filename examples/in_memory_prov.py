from flask import Flask
from flask.ext import ld
import sadi
from rdflib import *

prov = Namespace("http://www.w3.org/ns/prov#")

app = Flask(__name__)
app.config['DEBUG'] = True

db = ConjunctiveGraph()

vocab = Graph(db.store, prov)
vocab.load(prov)

@app.route("/classes", methods=['GET'])
@ld.sparql_select
def get_classes():
    return (db,'''select distinct ?class ?label where { ?class a owl:Class. ?class rdfs:label ?label.}''',{})

api = ld.LinkedDataApi(app, "/data", db.store, "http://localhost:5000")
api.create(prov.Agent, "agent")
api.create(prov.Entity, "entity")
api.create(OWL.Class, "class")

@app.route('/')
def hello_world():
    return 'Hello World!'

if __name__ == '__main__':
    app.run()