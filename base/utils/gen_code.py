import hashlib
import uuid
import datetime
import math, random, re
from hashlib import sha1
import os
from time import time

HASHKEY = "!%^*@GGJGggyjjgj14554^^ddy&^%$nnjbb###"

_sha1_re = re.compile(r'^[a-f0-9]{40}$')


def get_hash_password(password):
    data = password + HASHKEY
    data = data.encode()
    m = hashlib.md5()
    m.update(data)
    return m.hexdigest()


def generate_code(length=16, sortable=False):
    # 密码字符集，可任意添加你需要的字符
    chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
    code = ""

    # sortable prefix
    if sortable:
        seed = [-6, 10, 0]
        dateStr = datetime.datetime.now().strftime("%y%m%d")  # 161231

        for i in range(0, len(dateStr), 2):
            code += chars[int(dateStr[i:i + 2]) + seed[math.floor(i / 2)]]

    # random chars
    for i in range(len(code), length):
        r = random.choice(range(0, len(chars)))
        code += chars[r]

    return code


def get_csrftoken():
    return str(uuid.uuid1()).replace('-', '')


def getAESkey(key=None):
    if not key:
        return uuid.uuid4();
    else:
        if len(str(key)) == 16:
            return key
        if len(str(key)) < 16:
            return "0" * (16 - len(str(key))) + key;
        if len(str(key)) > 16:
            return str(key)[0:16]
    return


def _urandom():
    if hasattr(os, 'urandom'):
        return os.urandom(30)
    return str(random.random()).encode('ascii')


def generate_key(salt=None):
    if salt is None:
        salt = repr(salt).encode('ascii')
    return sha1(b''.join([
        salt,
        str(time()).encode('ascii'),
        _urandom()
    ])).hexdigest()


def is_valid_key(key):
    """Check if a key has the correct format."""
    return _sha1_re.match(key) is not None


if __name__ == "__main__":
    print(get_hash_password('123456'))
