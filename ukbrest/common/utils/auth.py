import os
import re

from flask_httpauth import HTTPBasicAuth
from ruamel.yaml import YAML
from werkzeug.security import check_password_hash, generate_password_hash

from ukbrest.config import logger


ENCODED_PASSWORD_PATTERN = re.compile('^pbkdf2:sha256:\d+\$\w+\$\w{64}$')


class PasswordHasher(object):
    def __init__(self, users_file, method='pbkdf2:sha256'):
        self.users_file = users_file
        self.method = method

    def verify_password(self, username, password):
        users_passwd = self.read_users_file()

        if users_passwd is None:
            return True

        if username in users_passwd:
            return check_password_hash(users_passwd.get(username), password)

        return False

    def read_users_file(self):
        self.process_users_file()
        return self._read_yaml_file(self.users_file)

    def _read_yaml_file(self, users_file):
        if users_file is None:
            return None

        if not os.path.isfile(users_file):
            return {}

        yaml = YAML()
        with open(users_file, 'r') as f:
            file_content = yaml.load(f)
            if file_content is None:
                file_content = {}
            return file_content

    def process_users_file(self):
        # process user/pass file
        yaml = YAML()

        if self.users_file is not None and os.path.isfile(self.users_file):
            users = self._read_yaml_file(self.users_file)

            # hash all non-hashed passwords
            new_users = {}
            for user, passw in users.items():
                if not ENCODED_PASSWORD_PATTERN.match(passw):
                    new_users[user] = generate_password_hash(passw, method=self.method)
                else:
                    new_users[user] = passw

            with open(self.users_file, 'w') as f:
                yaml.dump(new_users, f)
        elif self.users_file is not None and not os.path.isfile(self.users_file):
            logger.warning('Users file for authentication does not exist. No access will be allowed until the file is properly created.')


    def setup_http_basic_auth(self):
        auth = HTTPBasicAuth()
        self.verify_password = auth.verify_password(self.verify_password)
        return auth