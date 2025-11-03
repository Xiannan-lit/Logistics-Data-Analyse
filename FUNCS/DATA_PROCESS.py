import pandas as pd
from sqlalchemy import create_engine
import os
import re

user = os.getenv("DB_USER")  # 无默认值，不存在时返回 None
host = os.getenv("DB_HOST")  # 有默认值，不存在时返回 "localhost"
port = os.getenv("DB_PORT")
password = os.getenv("DB_PASSWORD")
name = os.getenv("DB_NAME")

engine = create_engine(F'mysql+pymysql://{user}:{password}@{host}:{port}/{name}')




def point_match(province,point,point_raw):
    if province=='/':
        return point_raw
    else:
        return point

def full_to_half(text):
    """
    将全角字符转换为半角字符
    :param text: 包含全角字符的字符串
    :return: 转换后的半角字符串
    """
    if not isinstance(text, str):
        return text  # 非字符串类型直接返回

    result = []

    for char in text:
        # 获取字符的Unicode编码
        code = ord(char)
        # 全角空格转换为半角空格
        if not char:
            return text
        if code == 0x3000:
            result.append(chr(0x20))
        # 全角字符（除空格外）转换为半角
        elif 0xFF01 <= code <= 0xFF5E:
            result.append(chr(code - 0xFEE0))
        # 其他字符不转换
        else:
            result.append(char)
    print('已替换成半角')
    return ''.join(result)