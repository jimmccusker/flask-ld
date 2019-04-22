from builtins import str
from rdflib import *

from wtforms import fields, validators, widgets

from flask_admin import form
from flask_admin.model.form import FieldPlaceholder
from flask_admin.model.fields import InlineFieldList, AjaxSelectField, AjaxSelectMultipleField
from flask_admin.model.widgets import InlineFormWidget
from flask_admin._compat import iteritems

import rdfalchemy
import re

dc = Namespace("http://purl.org/dc/terms/")

def get_label(r):
    label = r.label()
    if label == None or len(label) == 0:
        label = re.sub(":_-\/"," ",re.sub("^([^:])+:","",r.graph.qname(r.identifier)))
    return label



class RelationField(fields.SelectMultipleField):
    """
    A multiple-select, except displays a list of checkboxes.

    Iterating the field will produce subfields, allowing custom rendering of
    the enclosed checkbox fields.
    """
    def process_data(self,value):
        if value != None:
            self.data = [v.resUri for v in value]
        else:
            self.data = []

    choice_query = '''select ?id where { ?id a [rdfs:subClassOf* ?range].
    } order by ?id'''
    
    def iter_choices(self):
        range = self.rel.value(RDFS.range)
        bindings = dict(rel=self.rel.identifier)
        if range is not None:
            bindings['range'] = range.identifier
        for value,  in self.choice_graph.query(self.choice_query, initBindings=bindings):
            selected = self.coerce(value) in self.data
            label = get_label(self.choice_graph.resource(value))
            yield (value, label, selected)

    def pre_validate(self, form):
        if self.data:
            values = list(c[0] for c in self.iter_choices())
            for d in self.data:
                if d not in values:
                    raise ValueError(self.gettext("'%(value)s' is not a valid choice for this field") % dict(value=d))
            
    def __init__(self, rel, choice_graph, **kwargs):
        self.rel = rel
        self.choice_graph = choice_graph
        fields.SelectMultipleField.__init__(self, coerce=URIRef, **kwargs)

class TypeField(RelationField):
    """
    A multiple-select, except displays a list of checkboxes.

    Iterating the field will produce subfields, allowing custom rendering of
    the enclosed checkbox fields.
    """
    widget = widgets.ListWidget(prefix_label=False)
    option_widget = widgets.CheckboxInput()

    choice_query = '''select ?id where { ?id rdfs:subClassOf* ?rel. } order by ?id'''        

    def process_data(self,value):
        if value != None:
            self.data = [v.resUri for v in value]
        else:
            self.data = [self.rel.identifier]

mappings = {
    None: fields.TextField,
    XSD.string: fields.TextField,
    XSD.integer: fields.IntegerField,
    XSD.decimal: lambda *args, **kwargs: fields.DecimalField(*args, places=None, **kwargs),
    XSD.datetime: fields.DateTimeField,
    XSD.date: fields.DateField,
    XSD.boolean: fields.BooleanField
}

def get_field(model, p, field_name, field_args=None):
    if field_args == None: field_args = {}
    label = p.value(RDFS.label)
    if (label == None):
        label = re.sub(":_-\/"," ",re.sub("^([^:])+:","",p.graph.qname(p.identifier)))
    else:
        label = label.value
    description = p.value(dc.description)
    if description != None:
        description = description.value
    f = p.value(RDFS.range)
    if f != None:
        f = f.identifier
    #print field_name, f
    if p[RDF.type:OWL.ObjectProperty] and f:
        return RelationField(label=label, id=str(field_name),
                             rel=p, choice_graph=ConjunctiveGraph(model._local_api.store), **field_args)
    if f in mappings:
        return mappings[f](label=label,description=description, id=str(field_name), **field_args)
    else:
        return None


def get_form(model,
             base_class=form.BaseForm,
             only=None,
             exclude=None,
             field_args=None,
             extra_fields=None):
    """
    Create a wtforms Form for a given mongoengine Document schema::

        from flask_mongoengine.wtf import model_form
        from myproject.myapp.schemas import Article
        ArticleForm = model_form(Article)

    :param model:
        An RDFAlchemy class
    :param base_class:
        Base form class to extend from. Must be a ``wtforms.Form`` subclass.
    :param only:
        An optional iterable with the property names that should be included in
        the form. Only these properties will have fields.
    :param exclude:
        An optional iterable with the property names that should be excluded
        from the form. All other properties will have fields.
    :param field_args:
        An optional dictionary of field names mapping to keyword arguments used
        to construct each field object.
    :param converter:
        A converter to generate the fields based on the model properties. If
        not set, ``ModelConverter`` is used.
    """
    # if not isinstance(model, rdfalchemy.rdfSubject):
    #     raise TypeError('Model must be an RDFAlchemy rdf subject')

    field_args = field_args or {}

    # Find properties
    properties = list(model._sortable_columns.items())

    if only:
        props = dict(properties)

        def find(name):
            if extra_fields and name in extra_fields:
                return FieldPlaceholder(extra_fields[name])

            p = props.get(name)
            if p is not None:
                return p

            raise ValueError('Invalid model property name %s.%s' % (model, name))

        properties = ((p, find(p)) for p in only)

    elif exclude:
        properties = (p for p in properties if p[0] not in exclude)

    # Create fields
    field_dict = {"type":TypeField(rel=model.clResource, choice_graph=model.clResource.graph)}
    for name, p in properties:
        field = get_field(model, p, name, field_args.get(name))
        if field is not None:
            field_dict[name] = field

    # Contribute extra fields
    if not only and extra_fields:
        for name, field in iteritems(extra_fields):
            field_dict[name] = form.recreate_field(field)

    field_dict['model_class'] = model
    return type(model.__name__ + 'Form', (base_class,), field_dict)
