import os

INIT = 'init'
RUN = 'run'
MAKI = 'maki.yml'
JOB_FILE = 'job.yml'
SPARK_CONF = 'spark.yml'
AMATERASU_LOGO = 'banner2.txt'
APACHE_LOGO = 'apache.txt'
AMATERASU_TXT = 'amaterasu.txt'


class User:
    name = None
    email = None

    def __init__(self, name, email):
        self.name = name
        self.email = email


class Resources(dict):

    BASE_DIR = '{}/resources'.format(os.path.dirname(__file__))

    def __init__(self):
        super(Resources, self).__init__()
        for (_, _, files) in os.walk(self.BASE_DIR):
            for f in files:
                with open('{}/{}'.format(self.BASE_DIR, f), 'r') as fd:
                    if f != 'banner2.txt':
                        self[f] = fd.read()
                    else:
                        self[f] = fd.readlines()


RESOURCES = Resources()