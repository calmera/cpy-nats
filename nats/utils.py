import random

DIGITS = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'


def random_string(length=1):
    res = ''
    for i in range(length):
        res += DIGITS[random.randint(0, len(DIGITS))]
    return res
