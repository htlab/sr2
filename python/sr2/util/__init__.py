# -*- coding: utf-8 -*-
import random
import string


def gen_random_string(length):
    chars = string.ascii_letters + string.digits
    ret = ''
    for _ in xrange(length):
        ret = ret + random.choice(chars)
    return ret
