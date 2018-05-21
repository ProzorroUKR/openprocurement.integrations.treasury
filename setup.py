from setuptools import setup, find_packages
import os

version = '1.0'

requires = [
    'setuptools',
    'PyYAML',
    'chaussette',
    'gevent',
    'mock',
    'pyramid_exclog',
    'requests',
    'restkit',
    'retrying',
    'esculator',
    'iso8601',
    'pyramid',
    'pytz',
    'redis',
    'simplejson',
    'request_id_middleware',
    'server_cookie_middleware',
    'hypothesis'
]

test_requires = requires + [
    'webtest',
    'python-coveralls',
    'mock==1.0.1',
    'requests_mock==1.3.0',
    'bottle'
]

databridge_test_requires = requires + [
    'webtest',
    'python-coveralls',
    'mock==1.0.1',
    'requests_mock==1.3.0',
    'bottle'
]

databridge_requires = requires + [
    'PyYAML',
    'gevent',
    'LazyDB',
    'ExtendedJournalHandler',
    'requests',
    'restkit',
    'retrying',
    'esculator',
    'iso8601',
    'pytz',
    'redis',
    'openprocurement_client>=1.0b2'
]

api_test_requires = requires + [

]

api_requires = requires + [

]

entry_points = {
    'paste.app_factory': [
        'main = openprocurement.integrations.treasury:main'
    ],
    'console_scripts': [
        'integrations_treasury_bridge = openprocurement.integrations.treasury:main',
    ]
}

setup(
    name='openprocurement.integrations.treasury',
    version=version,
    description="",
    long_description=open("README.md").read(),
    classifiers=[
        "Framework :: Pylons",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
        "Topic :: Internet :: WWW/HTTP",
        "Topic :: Internet :: WWW/HTTP :: WSGI :: Application"
    ],
    keywords="web services",
    author='Quintagroup, Ltd.',
    author_email='info@quintagroup.com',
    license='Apache License 2.0',
    url='https://github.com/ITVaan/openprocurement.integrations.treasury',
    packages=find_packages(exclude=['ez_setup']),
    include_package_data=True,
    zip_safe=False,
    install_requires=requires,
    extras_require={
        'databridge': databridge_requires,
        'databridge_test': databridge_test_requires,
        'test': test_requires,
        'api': api_requires,
        'api_test': api_test_requires
    },
    entry_points=entry_points
)
