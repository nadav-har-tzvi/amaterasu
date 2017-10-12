"""
This is where the magic happens.
Basically, we have one handler for each subparser supported by the CLI.

Currently, we handle the following tasks:
1. Create a new Amaterasu repository
2. Read a maki.yml file and derive from it the relevant src and env files.
3. Run an Amaterasu pipeline, by invoking Amaterasu's scala code.

"""
import yaml

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


# region exceptions

class HandlerError(Exception):
    def __init__(self, *args, **kwargs):
        super(HandlerError, self).__init__(*args)
        self.errors = kwargs.get('inner_errors', [])

    def __str__(self):
        message = super(HandlerError, self).__str__()
        if self.errors:
            message += '\nEncountered the following errors: \n'
            for error in self.errors:
                message += str(error)
        return message


class ValidationError(Exception):
    pass

# endregion

# region bases and mixins


class BaseHandler(abc.ABC):

    def __init__(self, args):
        self.args = args

    @abc.abstractmethod
    def handle(self):
        pass


class BaseRepositoryHandler(BaseHandler):

    def __init__(self, args):
        super(BaseRepositoryHandler, self).__init__(args)
        self.dir_path = args.path if os.path.isabs(args.path) else os.path.abspath(args.path)
        self._validate_path()

    def _validate_path(self):
        root_dir_exists = os.path.exists(self.dir_path)
        if not root_dir_exists:
            base_path = os.path.split(self.dir_path)[0]
            if not os.path.exists(base_path):
                raise HandlerError("The base path: \"{}\" doesn't exist!".format(base_path))


class ValidateMakiMixin(object):

    @staticmethod
    def _validate_maki(maki):

        def str_ok(x):
            str_type = str if sys.version_info[0] > 2 else unicode
            return type(x) == str_type and len(x) > 0

        VALID_GROUPS = ['spark']
        VALID_TYPES = ['scala', 'sql', 'python', 'r']

        if not maki:
            raise HandlerError('Empty maki supplied')
        first_level_ok = 'job-name' in maki and 'flow' in maki
        if not first_level_ok:
            raise HandlerError('Invalid maki!')
        job_name_ok = str_ok(maki['job-name'])
        flow_ok = type(maki['flow']) == list and len(maki['flow']) > 0
        flow_steps_ok = True
        for step in maki['flow']:
            step_name_ok = lambda: 'name' in step and str_ok(step['name'])
            step_runner_ok = lambda: 'runner' in step and type(step['runner']) == dict \
                                     and 'group' in step['runner'] and str_ok(step['runner']['group']) \
                                     and step['runner']['group'] in VALID_GROUPS \
                                     and 'type' in step['runner'] and str_ok(step['runner']['type']) \
                                     and step['runner']['type'] in VALID_TYPES
            file_ok = lambda: 'file' in step and str_ok(step['file'])
            step_ok = type(step) == dict and step_name_ok() and step_runner_ok() and file_ok()
            if not step_ok:
                flow_steps_ok = False
                break
        return job_name_ok and flow_ok and flow_steps_ok
# endregion

# region implementations


class InitRepositoryHandler(BaseRepositoryHandler):
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


class UpdateRepositoryHandler(BaseRepositoryHandler, ValidateMakiMixin):
    """
    Handler that updates a repository based on its Maki.yml file
    Currently, it fills in the src directory with templates for the specified source files
    If a source file exists in the src directory that is not specified in the Maki.yml file,
    the user will be prompted to take action.
    """

    def _validate_repository(self):
        src_path = os.path.join(self.dir_path, 'src')
        env_path = os.path.join(self.dir_path, 'env')
        default_env_path = os.path.join(self.dir_path, 'env', 'default')
        errors = []
        if not os.path.exists(src_path):
            errors.append(ValidationError('Repository has no src directory'))
        if not os.path.exists(env_path):
            errors.append(ValidationError('Repository has no env directory'))
        if not os.path.exists(default_env_path):
            errors.append(ValidationError('Repository has no env/default directory'))
        return errors

    def _validate_path(self):
        super(UpdateRepositoryHandler, self)._validate_path()
        validation_errors = self._validate_repository()
        if validation_errors:
            raise HandlerError('Repository structure isn\'t valid!', inner_errors=validation_errors)

    def _load_existing_sources(self):
        return set(os.listdir(os.path.join(self.dir_path, 'src')))

    def _load_maki_sources(self):
        with open(os.path.join(self.dir_path, 'maki.yml'), 'r') as f:
            maki = yaml.load(f)
        UpdateRepositoryHandler._validate_maki(maki)
        source_files = {step['file'] for step in maki['flow']}
        return source_files

    def _write_sources_to_fs(self, sources):
        for file in sources:
            with open(os.path.join(self.dir_path, 'src', '{}'.format(file)), 'w'):
                pass

    def _get_user_input_for_source_not_on_maki(self, source): # This was separated out from _handle_source_not_on_maki so we can mock it
        print("The following source file: \"{}\" doesn't exist in the maki.yml file.".format(source))
        decision = input("[k]eep [d]elete [A]ll (e.g.: \"dA\" delete all): ").strip()
        while decision not in ['k', 'd', 'kA', 'dA']:
            print('Invalid choice "{}"')
            decision = input("[k]eep [d]elete [A]ll (e.g.: \"dA\" delete all): ")
        return decision

    def _handle_sources_not_on_maki(self, sources):
        """
        We ask the user to give us answers about sources that are not in the maki file.
        Currently, we only support either keeping them, or deleting them.
        :param sources:
        :return:
        """
        sources_iter = iter(sources)
        for source in sources_iter:
            decision = self._get_user_input_for_source_not_on_maki(source)
            if decision == 'dA':
                os.remove(os.path.join(self.dir_path, 'src', '{}'.format(source)))
                break
            elif decision == 'kA':
                return
            else:
                if decision == 'd':
                    os.remove(os.path.join(self.dir_path, 'src', '{}'.format(source)))
                else:
                    continue
        else:
            return

        # In case the user decided to do delete the rest in bulk:
        for source in sources_iter:
            os.remove(os.path.join(self.dir_path, 'src', '{}'.format(source)))

    def handle(self):
        """
        The idea is as following:
        Find all the sources that are present in the repository
        Find all the sources that are mentioned in the maki file
        If a source is mentioned in the maki and doesn't exist in the repository, create it
        If a source exists in the repository and doesn't exist in the maki, ask for user intervention
        :return:
        """
        existing_sources = self._load_existing_sources()
        maki_sources = self._load_maki_sources()
        sources_not_in_fs = maki_sources.difference(existing_sources)
        sources_not_in_maki = existing_sources.difference(maki_sources)
        self._write_sources_to_fs(sources_not_in_fs)
        self._handle_sources_not_on_maki(sources_not_in_maki)


class RunPipelineHandler(BaseHandler, ValidateMakiMixin):
    """
    This handler takes care of starting up Amaterasu Scala runtime.
    First, we validate the inputs we get. The user is expected to pass at least the repository URL.
    We inspect the submitted repository and validate that it exists and fits the structure of a valid Amaterasu job repository
    If all validations are passed, we invoke the Scala runtime.
    """

    def handle(self):
        pass


# endregion


# region entry point


def handle_args(args):
    if args.which == common.INIT:
        handler = InitRepositoryHandler(args)
    elif args.which == common.UPDATE:
        handler = UpdateRepositoryHandler(args)
    elif args.which == common.RUN:
        handler = RunPipelineHandler(args)
    else:
        raise NotImplemented("Unknown handler type: {}".format(args.which))
    handler.handle()

# endregion
