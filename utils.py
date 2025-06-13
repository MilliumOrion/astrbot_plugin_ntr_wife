import datetime
import os

from astrbot.api.all import *
from astrbot.api.event import AstrMessageEvent
from astrbot.api.star import StarTools

PLUGIN_NAME = 'astrbot_plugin_ntr_wife'
PLUGIN_DIR = StarTools.get_data_dir("astrbot_plugin_ntr_wife")
IMG_DIR = os.path.join(PLUGIN_DIR, 'img', 'wife')
SQLITE_FILE = os.path.join(PLUGIN_DIR, 'wife_history.db')
os.makedirs(IMG_DIR, exist_ok=True)


def get_today():
    """获取当前上海时区日期字符串"""
    utc_now = datetime.datetime.now(datetime.timezone.utc)
    # 将时间调整为东八区，并设置为今天的0点0分0秒
    east_8_midnight = (utc_now + datetime.timedelta(hours=8)).date()
    return int(datetime.datetime.combine(east_8_midnight, datetime.time(0, 0, 0)).timestamp())


def parse_target_uid(event: AstrMessageEvent):
    for seg in event.get_messages():
        if isinstance(seg, At):
            qq = str(seg.qq)
            if qq == event.get_self_id():
                continue
            name = seg.name if seg.name else qq
            return qq, name
    return "", ""


def parse_wife_name(file_name: str) -> str:
    if not file_name:
        return ""
    return os.path.splitext(file_name)[0]
