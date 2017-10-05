import os
import pygit2
import common


class AmaRepository:

    def __init__(self, root_path, user_info):
        """

        :param root_path:
        :param user_info:
        :type user_info: common.User
        """
        self.root_path = root_path
        self.src_path = os.path.abspath('{}/src'.format(root_path))
        self.env_path = os.path.abspath('{}/env'.format(root_path))
        self.git_repository = None
        self.signature = pygit2.Signature(user_info.name, user_info.email)

    @property
    def exists(self):
        return os.path.exists('{}/.git'.format(self.root_path))

    def _build_tree(self):
        default_env = os.path.abspath('{}/default'.format(self.env_path))
        os.makedirs(self.src_path, exist_ok=True)
        os.makedirs(self.env_path, exist_ok=True)
        os.makedirs(default_env, exist_ok=True)
        if not os.path.exists('{}/{}'.format(self.root_path, common.MAKI)):
            with open('{}/{}'.format(self.root_path, common.MAKI), 'w') as f:
                f.write(common.RESOURCES[common.MAKI])
        if not os.path.exists('{}/{}'.format(default_env, common.JOB_FILE)):
            with open('{}/{}'.format(default_env, common.JOB_FILE), 'w') as f:
                f.write(common.RESOURCES[common.JOB_FILE])
        if not os.path.exists('{}/{}'.format(default_env, common.SPARK_CONF)):
            with open('{}/{}'.format(default_env, common.SPARK_CONF), 'w') as f:
                f.write(common.RESOURCES[common.SPARK_CONF])


    def build(self):
        self.git_repository = pygit2.init_repository(self.root_path)
        self._build_tree()
        self.git_repository.index.add_all()

    def commit(self):
        tree = self.git_repository.TreeBuilder().write()
        self.git_repository.create_commit('refs/heads/master', self.signature, self.signature, "Amaterasu job repo init", tree, [])