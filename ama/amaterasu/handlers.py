"""
This is where the magic happens.
Basically, we have one handler for each subparser supported by the CLI.

Currently, we handle the following tasks:
1. Create a new Amaterasu repository
2. Read a maki.yml file and derive from it the relevant src and env files.
3. Run an Amaterasu pipeline, by invoking Amaterasu's scala code.

"""
from . import common
from .compat import *
from six.moves import configparser, cStringIO, input
from amaterasu.repository import AmaRepository
import abc
import os
import sys


if sys.version_info[0] == 2:
    git_parser = configparser.SafeConfigParser()
    data = cStringIO()
    data.write('\n'.join(line.strip() for line in open(os.path.expanduser('~/.gitconfig'))))
    data.seek(0)
    git_parser.readfp(data)
else:
    git_parser = configparser.ConfigParser()
    git_parser.read(os.path.expanduser('~/.gitconfig'))


class HandlerError(Exception):
    pass


class BaseHandler(abc.ABC):

    def __init__(self, args):
        self.args = args

    @abc.abstractmethod
    def handle(self):
        pass


class InitRepositoryHandler(BaseHandler):
    """
    A handler for creating a new Amaterasu repository
    We generate the following structure:
    /root_dir
    |__/src ## This is where the source code resides
    |   |
    |   |__task1.scala
    |   |
    |   |__task2.py
    |   |
    |   |__task3.sql
    |
    |__/env ## This is a configuration directory for each environment the user defines, there should be a "default" env.
    |   |
    |   |__/default
    |   |  |
    |   |  |__job.yml
    |   |  |
    |   |  |__spark.yml
    |   |
    |   |__/test
    |
    |__maki.yml ## The job definition
    """

    def __init__(self, args):
        """
        Expects a valid path that will serve as the root directory for the created repository.
        We run a validation function on the path to make sure it exists and that it is not already a git repository.
        :param args:
        """
        super(InitRepositoryHandler, self).__init__(args)
        self.dir_path = args.path if os.path.isabs(args.path) else os.path.abspath(args.path)
        self._validate_path()

    def _validate_path(self):
        root_dir_exists = os.path.exists(self.dir_path)
        if not root_dir_exists:
            base_path = os.path.split(self.dir_path)[0]
            if not os.path.exists(base_path):
                raise HandlerError("The base path: \"{}\" doesn't exist!".format(base_path))

    @staticmethod
    def _config_user():
        """
        First we try to get the user details from the global .gitconfig
        If we fail at that, then we will ask the user for his credentials
        :return:
        """
        try:
            username = git_parser.get('user', 'name')
        except KeyError:
            username = ''
        try:
            email = git_parser.get('user', 'email')
        except KeyError:
            email = ''

        new_name = input("Your name [{}]: ".format(username))
        if new_name == username == '':
            raise HandlerError('Username is required!')
        elif new_name == '':
            new_name = username

        new_email = input("Your email [{}]:".format(email))
        if new_email == email == '':
            raise HandlerError('Email is required!')
        elif new_email == '':
            new_email = email

        return common.User(new_name, new_email)

    def handle(self):
        print("Setting up an Amaterasu job repository at {}".format(self.dir_path))
        user = self._config_user()
        repo = AmaRepository(self.dir_path, user)
        repo.build()
        repo.commit()
        print("Amaterasu job repository set up successfully")


class RunPipelineHandler(BaseHandler):

    def handle(self):
        pass


def handle_args(args):
    if args.which == common.INIT:
        handler = InitRepositoryHandler(args)
    elif args.which == common.RUN:
        handler = RunPipelineHandler(args)
    else:
        raise NotImplemented("Unknown handler type: {}".format(args.which))
    handler.handle()
