import os
import unittest
from shutil import copyfile

from ruamel.yaml import YAML
from ukbrest.common.utils.auth import PasswordHasher
from tests.utils import get_repository_path


class WSGIFunctions(unittest.TestCase):
    def load_data(self, filepath):
        yaml = YAML()
        with open(filepath, 'r') as f:
            return yaml.load(f)

    def test_process_users_file_test00(self):
        # prepare
        orig_user_file = get_repository_path('wsgi/test00/users.txt')
        users_file = orig_user_file + '.bak'
        copyfile(orig_user_file, users_file)

        orig_users = self.load_data(orig_user_file)

        # run
        ph = PasswordHasher(users_file, method='pbkdf2:sha256')
        ph.process_users_file()

        # evaluate
        assert os.path.isfile(users_file)

        users = self.load_data(users_file)

        assert len(users) == 3
        for user, password in users.items():
            assert user in orig_users.keys(), user
            assert password != orig_users[user], password + ' / ' + orig_users[user]
            assert len(password) == 93, (len(password), password)

        os.remove(users_file)

    def test_process_users_file_file_does_not_exist_test00(self):
        # prepare
        users_file = get_repository_path('no/existing/file/here.txt')

        # run
        ph = PasswordHasher(users_file, method='pbkdf2:sha256')
        ph.process_users_file()

    def test_process_users_file_already_hashed_test00(self):
        # prepare
        orig_user_file = get_repository_path('wsgi/test00/users.txt')
        users_file = orig_user_file + '.bak'
        copyfile(orig_user_file, users_file)

        orig_users = self.load_data(orig_user_file)
        ph = PasswordHasher(users_file, method='pbkdf2:sha256')
        ph.process_users_file()
        users = self.load_data(users_file)

        # run
        ph = PasswordHasher(users_file, method='pbkdf2:sha256')
        ph.process_users_file()

        # evaluate
        assert os.path.isfile(users_file)

        new_users = self.load_data(users_file)

        assert len(users) == 3
        for user, password in new_users.items():
            assert user in orig_users.keys(), user
            assert password == users[user], password + ' / ' + users[user]
            assert len(password) == 93, (len(password), password)

        os.remove(users_file)

    def test_process_users_file_one_password_hashed_rest_not_test01(self):
        # prepare
        orig_user_file = get_repository_path('wsgi/test01/users.txt')
        users_file = orig_user_file + '.bak'
        copyfile(orig_user_file, users_file)

        orig_users = self.load_data(orig_user_file)

        # run
        ph = PasswordHasher(users_file, method='pbkdf2:sha256')
        ph.process_users_file()

        # evaluate
        assert os.path.isfile(users_file)

        users = self.load_data(users_file)

        assert len(users) == 3
        for user, password in users.items():
            assert user in orig_users.keys(), user

            if user != 'adams':
                assert password != orig_users[user], user + ' / ' + password + ' / ' + orig_users[user]
            else:
                assert password == users[user], user +  password + ' / ' + users[user]

            assert len(password) == 93, (len(password), password)

        os.remove(users_file)

    def test_verify_password_test01(self):
        # prepare
        orig_user_file = get_repository_path('wsgi/test01/users.txt')
        users_file = orig_user_file + '.bak'
        copyfile(orig_user_file, users_file)

        ph = PasswordHasher(users_file, method='pbkdf2:sha256')
        ph.process_users_file()

        # evaluate
        assert os.path.isfile(users_file)
        assert not ph.verify_password('milton', 'whatever')
        assert ph.verify_password('john', 'mypassword')
        assert ph.verify_password('adams', 'anotherpassword')
        assert ph.verify_password('james', 'mypassword')

        os.remove(users_file)

    def test_verify_password_users_file_does_not_exist_test01(self):
        # prepare
        users_file = get_repository_path('no/existing/file/here.txt')

        ph = PasswordHasher(users_file, method='pbkdf2:sha256')
        ph.process_users_file()

        # evaluate
        assert not ph.verify_password('milton', 'whatever')
        assert not ph.verify_password('john', 'mypassword')
        assert not ph.verify_password('adams', 'anotherpassword')
        assert not ph.verify_password('james', 'mypassword')

    def test_verify_password_users_file_empty_test01(self):
        # prepare
        orig_user_file = get_repository_path('wsgi/test02/users.txt')
        users_file = orig_user_file + '.bak'

        ph = PasswordHasher(users_file, method='pbkdf2:sha256')
        ph.process_users_file()

        # evaluate
        assert not ph.verify_password('milton', 'whatever')
        assert not ph.verify_password('john', 'mypassword')
        assert not ph.verify_password('adams', 'anotherpassword')
        assert not ph.verify_password('james', 'mypassword')

    def test_verify_password_users_file_none_test01(self):
        # prepare
        ph = PasswordHasher(None, method='pbkdf2:sha256')
        ph.process_users_file()

        # evaluate
        assert ph.verify_password('milton', 'whatever')
        assert ph.verify_password('john', 'mypassword')
        assert ph.verify_password('adams', 'anotherpassword')
        assert ph.verify_password('james', 'mypassword')
