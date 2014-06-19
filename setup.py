# -*- coding: utf-8 -*-
#

#This is just a work-around for a Python2.7 issue causing
#interpreter crash at exit when trying to log an info message.
try:
    import logging
    import multiprocessing
except:
    pass

import sys
py_version = sys.version_info[:2]

try:
    from setuptools import setup, find_packages
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages

testpkgs=['flask-webtest',
               'nose',
               'coverage'
               ]

install_requires=[
    "rdfalchemy",
    "Flask",
    "flask-security",
    'Flask-RESTful',
    ]

setup(
    name='flask-ld',
    version='0.1',
    description='',
    author='Jim McCusker',
    author_email='mccusker@gmail.com',
    #url='',
    packages=find_packages(exclude=['ez_setup']),
    install_requires=install_requires + testpkgs,
    include_package_data=True,
    test_suite='nose.collector',
    tests_require=testpkgs,
    package_data={'pywebauth': ['i18n/*/LC_MESSAGES/*.mo',
                                 'templates/*/*',
                                 'public/*/*']},
    message_extractors={'flask-ld': [
            ('**.py', 'python', None)]},
    zip_safe=False
)
