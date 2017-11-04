from behave import *
from amaterasu import common, handlers
from tests.utils import MockArgs
import os
import pygit2

use_step_matcher("re")


def mock_git_clone(context, url):
    repository_dest = os.path.abspath('tmp/amaterasu')
    if url == 'http://git.sunagakure.com/ama-job-non-exist.git':
        raise pygit2.GitError("failed to send request: The server name or address could not be resolved")
    elif url == "http://git.sunagakure.com/ama-job-valid.git":
        os.mkdir(repository_dest)
        os.mkdir(os.path.join(repository_dest, 'src'))
        os.mkdir(os.path.join(repository_dest, 'env'))
        os.mkdir(os.path.join(repository_dest, 'env', 'default'))
        with open(os.path.join(repository_dest, common.MAKI), 'w') as maki:
            maki.write(context.test_resources[common.MAKI])
        with open(os.path.join(repository_dest, 'env', 'default', common.SPARK_CONF), 'w') as spark:
            spark.write(context.test_resources[common.SPARK_CONF])
        with open(os.path.join(repository_dest, 'env', 'default', common.JOB_FILE), 'w') as spark:
            spark.write(context.test_resources[common.JOB_FILE])
    elif url == 'http://git.sunagakure.com/some-repo.git':
        os.mkdir(repository_dest)
        os.mkdir(os.path.join(repository_dest, 'sasuke'))
        os.mkdir(os.path.join(repository_dest, 'sasuke', 'is'))
        os.mkdir(os.path.join(repository_dest, 'sasuke', 'is', 'lame'))
    else:
        raise NotImplemented()


@given("A valid repository")
def step_impl(context):
    """
    :type context: behave.runner.Context
    """
    context.repository_uri = 'http://git.sunagakure.com/ama-job-valid.git'


@when("Running a pipeline with the given repository")
def step_impl(context):
    """
    :type context: behave.runner.Context
    """
    args = MockArgs(repository=context.repository_uri)

    handlers.RunPipelineHandler(args)


@given("A valid file URI repository")
def step_impl(context):
    """
    :type context: behave.runner.Context
    """
    pass


@given("A repository that doesn't exist")
def step_impl(context):
    """
    :type context: behave.runner.Context
    """
    context.repository_uri = 'http://git.sunagakure.com/ama-job-non-exist.git'


@given("A repository that is not Amaterasu compliant")
def step_impl(context):
    """
    :type context: behave.runner.Context
    """
    context.repository_uri = 'http://git.sunagakure.com/some-repo.git'