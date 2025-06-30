import asyncio
import os
import random
import traceback
from typing import Dict

import aiosqlite
from apscheduler.schedulers.background import BackgroundScheduler
from astrbot.api import logger
from astrbot.api.all import *
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, register

from .db import (
    GroupConfig,
    SwapRequest,
    UserCount,
    UserWife,
    UserWifeHisotry,
    Wife,
    WifeCount,
)
from .utils import IMG_DIR, SQLITE_FILE, get_today, parse_target_uid, parse_wife_name


@register("astrbot_plugin_ntr_wife", "MilliumOrion", "群二次元老婆插件", "1.0.0")
class NtrPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.group_lock: Dict[str, asyncio.Lock] = {}  # 每个群一把锁
        self.group_create_lock: asyncio.Lock = asyncio.Lock()
        self.config = config
        self.ntr_max_per_day = config.get("ntr_max_per_day")  # 每天最多ntr次数
        self.ntr_possibility = config.get("ntr_possibility")  # ntr成功率
        self.change_max_per_day = config.get("change_max_per_day")  # 每天最多抽老婆次数
        self.swap_max_per_day = config.get("swap_max_per_day")  # 每天最多更换老婆次数

    async def initialize(self):
        # 初始化数据库
        await self.initialize_db()
        # 定时清理过期数据
        await self.initialize_today_data()
        self.scheduler = BackgroundScheduler()
        self.scheduler.add_job(self.initialize_today_data, "cron", hour=0, minute=0)
        # 启动调度器
        self.scheduler.start()
        logger.info(f"astrbot_plugin_ntr_wife initialized.")

    # 初始化数据库
    async def initialize_db(self):
        try:
            async with aiosqlite.connect(SQLITE_FILE) as sql_conn:
                async with sql_conn.cursor() as cursor:
                    await UserWife.init_table(cursor)
                    await SwapRequest.init_table(cursor)
                    await GroupConfig.init_table(cursor)
                    await UserCount.init_table(cursor)
                    await UserWifeHisotry.init_table(cursor)
                    await WifeCount.init_table(cursor)
                    wife_imgs = os.listdir(IMG_DIR)  # 获取老婆图片文件夹中的所有图片
                    await Wife.init_table(cursor, wife_imgs)
                    logger.info(f"loaded {len(wife_imgs)} wife images")
                    await sql_conn.commit()
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}" + traceback.format_exc())

    # 清理过期数据
    async def initialize_today_data(self):
        today = get_today()
        try:
            async with aiosqlite.connect(SQLITE_FILE) as sql_conn:
                async with sql_conn.cursor() as cursor:
                    # 清理过期数据
                    await UserCount.clear_expired(cursor, today)
                    await sql_conn.commit()
                    logger.info("Expired data cleared successfully")
        except Exception as e:
            logger.error(f"Failed to clear expired data: {e}" + traceback.format_exc())

    async def _get_group_lock(self, gid: str):
        if gid not in self.group_lock:
            await self.group_create_lock.acquire()
            if gid not in self.group_lock:
                self.group_lock[gid] = asyncio.Lock()
            self.group_create_lock.release()
        return self.group_lock[gid]

    @filter.command("抽老婆")
    async def animewife(self, event: AstrMessageEvent):
        gid = str(event.message_obj.group_id)
        if not gid:
            yield event.plain_result("只能在群聊中使用")
            return
        uid = str(event.get_sender_id())
        today = get_today()
        lock = await self._get_group_lock(gid)
        async with lock:
            # 获取用户今天抽的老婆
            try:
                async with aiosqlite.connect(SQLITE_FILE) as sql_conn:
                    async with sql_conn.cursor() as cursor:
                        # 今天已经抽老婆的次数
                        user_count = await UserCount.get_count(cursor, gid, uid, today)
                        if user_count.change_count >= self.change_max_per_day:
                            yield event.chain_result([Plain(f"今天已经换过{self.change_max_per_day}次老婆啦！")])
                            return

                        # 选择老婆
                        wife_file = await UserWife.get_random_wife(cursor, gid)
                        if not wife_file:
                            yield event.chain_result([Plain(f"今天群友已经把所有老婆抽走了！")])
                            return

                        # 登记新老婆
                        wife_name = parse_wife_name(wife_file)
                        await UserWife(gid=gid, uid=uid, wife=wife_file).save_user_wife(cursor)
                        is_first_get = await UserWifeHisotry.add_wife_histroy(cursor, uid, wife_name)
                        # 老婆的使用信息
                        wife_count = await WifeCount.get_count(cursor, gid, wife_name)
                        await WifeCount.increase_count(cursor, gid, wife_name, "draw_count")
                        # 更新次数
                        await UserCount.increase_count(cursor, gid, uid, today, "change_count")

                        await sql_conn.commit()

                        text = "{}你今天的二次元老婆是【{}】{}哒~".format(
                            "出新了！\n" if is_first_get else "",
                            wife_name,
                            "(赞新出厂)" if not wife_count else "",
                        )
                        wife_path = os.path.join(IMG_DIR, wife_file)
                        if os.path.exists(wife_path):
                            chain = [Plain(text), Image.fromFileSystem(wife_path)]
                        else:
                            chain = [
                                Plain(text),
                                Plain(f"老婆的图片丢失了，请联系管理员"),
                            ]
                        if wife_count:
                            chain.append(Plain(f"\n抽到次数：{wife_count.draw_count}\n牛到次数：{wife_count.ntr_count}\n离婚次数：{wife_count.divorce_count}\n交换次数：{wife_count.swap_count}"))
                        yield event.chain_result(chain)
                        return
            except Exception as e:
                logger.error(f"抽老婆失败，{e}" + traceback.format_exc())
                yield event.chain_result([Plain("抽老婆失败！请联系管理员")])

    @filter.command("离婚")
    async def divorce(self, event: AstrMessageEvent):
        gid = str(event.message_obj.group_id)
        if not gid:
            yield event.plain_result("只能在群聊中使用")
            return
        uid = str(event.get_sender_id())

        try:
            async with aiosqlite.connect(SQLITE_FILE) as sql_conn:
                async with sql_conn.cursor() as cursor:
                    # 查老婆
                    user_wife = await UserWife.get_user_wife(cursor, gid, uid)
                    wife_file = user_wife.wife
                    if not wife_file:
                        yield event.chain_result([Plain(f"单身狗也配离婚？")])
                        return

                    # 离婚
                    wife_name = parse_wife_name(wife_file)
                    await UserWife(gid=gid, uid=uid, wife="").save_user_wife(cursor)
                    await WifeCount.increase_count(cursor, gid, wife_name, "divorce_count")
                    await sql_conn.commit()

                    yield event.chain_result([Plain(f"你与老婆【{wife_name}】离婚成功")])
                    return
        except Exception as e:
            logger.error(f"离婚失败，{e}" + traceback.format_exc())
            yield event.chain_result([Plain("离婚失败！请联系管理员")])

    @filter.command("牛老婆")
    async def ntr_wife(self, event: AstrMessageEvent):
        gid = str(event.message_obj.group_id)
        if not gid:
            yield event.chain_result("只能在群聊中使用")
            return
        uid = str(event.get_sender_id())
        tid, tname = parse_target_uid(event)
        if not tid or tid == uid:
            yield event.chain_result([Plain("请指定目标" if not tid else "不能牛自己")])
            return
        today = get_today()
        try:
            async with aiosqlite.connect(SQLITE_FILE) as sql_conn:
                async with sql_conn.cursor() as cursor:
                    enable_ntr = await GroupConfig.is_group_ntr_enable(cursor, gid)
                    if not enable_ntr:
                        yield event.plain_result("牛老婆功能未开启！")
                        return

                    # 今天已经ntr的次数
                    user_count = await UserCount.get_count(cursor, gid, uid, today)
                    if user_count.ntr_count >= self.ntr_max_per_day:
                        yield event.chain_result([Plain(f"每日最多{self.ntr_max_per_day}次，明天再来~")])
                        return

                    # 目标老婆
                    target_wife = await UserWife.get_user_wife(cursor, gid, tid)
                    if not target_wife.wife:
                        yield event.chain_result([Plain(f"{tname}今天还没有老婆")])
                        return

                    # 更新牛次数
                    await UserCount.increase_count(cursor, gid, uid, today, "ntr_count")
                    # 牛
                    if random.random() < self.ntr_possibility:
                        target_wife_name = parse_wife_name(target_wife.wife)
                        await UserWife(gid=gid, uid=uid, wife=target_wife.wife).save_user_wife(cursor)
                        await UserWife(gid=gid, uid=tid, wife="").save_user_wife(cursor)
                        await WifeCount.increase_count(cursor, gid, target_wife_name, "ntr_count")
                        await sql_conn.commit()
                        chain = [Plain(f"牛{tname}的老婆成功！你现在的老婆是【{target_wife_name}】")]
                    else:
                        await sql_conn.commit()
                        chain = [Plain(f"牛{tname}的老婆失败！剩余次数{self.ntr_max_per_day - user_count.ntr_count - 1}")]
                    yield event.chain_result(chain)
                    return
        except Exception as e:
            logger.error(f"牛老婆失败，{e}" + traceback.format_exc())
            yield event.chain_result([Plain("牛老婆失败！请联系管理员")])

    @filter.command("查老婆")
    async def search_wife(self, event: AstrMessageEvent):
        gid = str(event.message_obj.group_id)
        if not gid:
            yield event.chain_result("只能在群聊中使用")
            return
        uid = str(event.get_sender_id())
        tid, tname = parse_target_uid(event)
        if not tid:
            # 没有指定人，就查自己
            tid = uid
            tname = "你"
        try:
            async with aiosqlite.connect(SQLITE_FILE) as sql_conn:
                async with sql_conn.cursor() as cursor:
                    # 查
                    user_wife = await UserWife.get_user_wife(cursor, gid, tid)
                    if not user_wife.wife:
                        yield event.chain_result([Plain(f"没有找到{tname}的老婆信息")])
                        return
                    wife_path = os.path.join(IMG_DIR, user_wife.wife)
                    wife_name = parse_wife_name(user_wife.wife)
                    text = f"{tname}的老婆是【{wife_name}】~"
                    if os.path.exists(wife_path):
                        chain = [Plain(text), Image.fromFileSystem(wife_path)]
                    else:
                        chain = [Plain(text), Plain(f"老婆的图片丢失了，请联系管理员")]
                    yield event.chain_result(chain)
                    return
        except Exception as e:
            logger.error(f"查老婆失败，{e}" + traceback.format_exc())
            yield event.chain_result([Plain("查老婆失败！请联系管理员")])

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("切换ntr状态")
    async def switch_ntr(self, event: AstrMessageEvent):
        gid = str(event.message_obj.group_id)
        if not gid:
            yield event.chain_result("只能在群聊中使用")
            return
        try:
            async with aiosqlite.connect(SQLITE_FILE) as sql_conn:
                async with sql_conn.cursor() as cursor:
                    enable_ntr = await GroupConfig.is_group_ntr_enable(cursor, gid)
                    await GroupConfig.set_group_ntr(cursor, gid, 0 if enable_ntr else 1)
                    await sql_conn.commit()
                    state = "开启" if not enable_ntr else "关闭"
                    yield event.plain_result(f"NTR功能已{state}")
                    return
        except Exception as e:
            logger.error(f"切换ntr状态失败，{e}" + traceback.format_exc())
            yield event.plain_result("切换ntr状态失败！请联系管理员")

    @filter.command("换老婆")
    async def swap_wife(self, event: AstrMessageEvent):
        gid = str(event.message_obj.group_id)
        if not gid:
            yield event.chain_result("只能在群聊中使用")
            return
        sid = str(event.get_sender_id())
        sname = event.get_sender_name()
        tid, tname = parse_target_uid(event)
        if not tid:
            yield event.chain_result([Plain("请指定要换谁的老婆")])
            return
        if tid == sid:
            yield event.chain_result([Plain("不能和自己换老婆")])
            return
        today = get_today()

        try:
            async with aiosqlite.connect(SQLITE_FILE) as sql_conn:
                async with sql_conn.cursor() as cursor:
                    # 今天已经交换的次数
                    user_count = await UserCount.get_count(cursor, gid, sid, today)
                    if user_count.swap_count >= self.swap_max_per_day:
                        yield event.chain_result([Plain(f"每日最多{self.swap_max_per_day}次，明天再来~")])
                        return

                    # 自己的老婆
                    source_wife = await UserWife.get_user_wife(cursor, gid, sid)
                    if not source_wife.wife:
                        yield event.chain_result([Plain("你还没有老婆，请先抽老婆！")])
                        return
                    source_wife_name = parse_wife_name(source_wife.wife)

                    # 别人的老婆
                    target_wife = await UserWife.get_user_wife(cursor, gid, tid)
                    if not target_wife.wife:
                        yield event.chain_result([Plain(f"{tname}还没有老婆！")])
                        return
                    source_wife_name = parse_wife_name(source_wife.wife)
                    target_wife_name = parse_wife_name(target_wife.wife)

                    # 登记交换
                    await SwapRequest(
                        gid=gid,
                        source_user=sid,
                        target_user=tid,
                        source_wife=source_wife.wife,
                        target_wife=target_wife.wife,
                        source_user_name=sname,
                        target_user_name=tname,
                    ).save_request(cursor)

                    await UserCount.increase_count(cursor, gid, sid, today, "swap_count")
                    await sql_conn.commit()
                    # 请求交换
                    await event.send(
                        MessageChain(
                            [
                                At(qq=tid),
                                Plain(f"{sname}请求与你换老婆,TA的老婆是【{source_wife_name}】,你的老婆是【{target_wife_name}】。请回复：\n同意交换 @发起者\n拒绝交换 @发起者"),
                            ]
                        )
                    )
                    return
        except Exception as e:
            logger.error(f"换老婆失败，{e}" + traceback.format_exc())
            yield event.chain_result([Plain("换老婆失败！请联系管理员")])

    @filter.command("同意交换")
    async def agree_swap_wife(self, event: AstrMessageEvent):
        gid = str(event.message_obj.group_id)
        if not gid:
            yield event.chain_result("只能在群聊中使用")
            return
        tid = str(event.get_sender_id())
        tname = event.get_sender_name()
        sid, sname = parse_target_uid(event)
        if not sid:
            yield event.chain_result([Plain("请指定要换谁的老婆")])
            return
        if sid == tid:
            yield event.chain_result([Plain("不能和自己换老婆")])
            return

        try:
            async with aiosqlite.connect(SQLITE_FILE) as sql_conn:
                async with sql_conn.cursor() as cursor:
                    # 获取对方登记交换的老婆
                    swap_history = await SwapRequest.get(cursor, gid, sid, tid)
                    prepare_source_wife_name = parse_wife_name(swap_history.source_wife)
                    prepare_target_wife_name = parse_wife_name(swap_history.target_wife)
                    if not swap_history.target_wife:
                        yield event.chain_result([Plain(f"{sname}并不准备和你换老婆")])
                        return

                    # 获取自己的老婆
                    target_wife = await UserWife.get_user_wife(cursor, gid, tid)
                    target_wife_name = parse_wife_name(target_wife.wife)
                    # 获取对方的老婆
                    source_wife = await UserWife.get_user_wife(cursor, gid, sid)
                    source_wife_name = parse_wife_name(source_wife.wife)

                    # 删除交换记录
                    await SwapRequest.delete_request(cursor, gid, sid, tid)

                    if target_wife_name != prepare_target_wife_name:
                        await sql_conn.commit()
                        if target_wife_name:
                            yield event.chain_result([Plain(f"{sname}想换的老婆是【{prepare_target_wife_name}】,而你现在的老婆是【{target_wife_name}】，交换失败。")])
                        else:
                            yield event.chain_result([Plain(f"{sname}想换的老婆是【{prepare_target_wife_name}】,而你现在没老婆，交换失败。")])
                        return

                    if source_wife_name != prepare_source_wife_name:
                        await sql_conn.commit()
                        if source_wife_name:
                            yield event.chain_result(
                                [
                                    Plain(f"{sname}承诺拿来交换的老婆是【{prepare_source_wife_name}】,而对方现在的老婆是【{source_wife_name}】，交换失败。"),
                                ]
                            )
                        else:
                            yield event.chain_result(
                                [
                                    Plain(f"{sname}承诺拿来交换的老婆是【{prepare_source_wife_name}】,而对方现在没老婆，交换失败。"),
                                ]
                            )
                        return

                    # 交换老婆
                    await UserWife(gid=gid, uid=tid, wife=source_wife.wife).save_user_wife(cursor)
                    await UserWife(gid=gid, uid=sid, wife=target_wife.wife).save_user_wife(cursor)
                    await WifeCount.increase_count(cursor, gid, source_wife_name, "swap_count")
                    await WifeCount.increase_count(cursor, gid, target_wife_name, "swap_count")
                    await sql_conn.commit()
                    yield event.chain_result(
                        [
                            Plain(f"交换老婆成功。\n{tname}现在的老婆是【{source_wife_name}】。\n{sname}现在的老婆是【{target_wife_name}】。"),
                        ]
                    )
                    return
        except Exception as e:
            logger.error(f"同意换老婆失败，{e}" + traceback.format_exc())
            yield event.chain_result([Plain("换老婆失败！请联系管理员")])

    @filter.command("拒绝交换")
    async def reject_swap_wife(self, event: AstrMessageEvent):
        gid = str(event.message_obj.group_id)
        if not gid:
            yield event.chain_result("只能在群聊中使用")
            return
        tid = str(event.get_sender_id())
        tname = event.get_sender_name()
        sid, _ = parse_target_uid(event)
        if not sid:
            yield event.chain_result([Plain("请指定要决绝谁的交换请求")])
            return
        if sid == tid:
            yield event.chain_result([Plain("不能拒绝自己")])
            return

        try:
            async with aiosqlite.connect(SQLITE_FILE) as sql_conn:
                async with sql_conn.cursor() as cursor:
                    # 获取对方登记交换的老婆
                    swap_history = await SwapRequest.get(cursor, gid, sid, tid)
                    if not swap_history:
                        yield event.chain_result([Plain("没有找到对方的交换请求")])
                        return
                    # 删除对方登记的交换记录
                    await SwapRequest.delete_request(cursor, gid, sid, tid)
                    await sql_conn.commit()
                    await event.send(MessageChain([At(qq=sid), Plain(f"{tname}拒绝了你的交换请求。")]))

        except Exception as e:
            logger.error(f"拒绝换老婆失败，{e}" + traceback.format_exc())
            yield event.chain_result([Plain("换老婆失败！请联系管理员")])

    @filter.command("查看交换请求")
    async def view_swap_requests(self, event: AstrMessageEvent):
        gid = str(event.message_obj.group_id)
        if not gid:
            yield event.chain_result("只能在群聊中使用")
            return
        uid = str(event.get_sender_id())

        try:
            async with aiosqlite.connect(SQLITE_FILE) as sql_conn:
                async with sql_conn.cursor() as cursor:
                    # 获取记录
                    swap_by_self = await SwapRequest.list_swap_request(cursor, gid, uid, "")
                    swap_by_other = await SwapRequest.list_swap_request(cursor, gid, "", uid)
                    if not swap_by_self and not swap_by_other:
                        await sql_conn.commit()
                        yield event.chain_result([Plain("没有任何交换请求")])
                        return

                    parts = []
                    if swap_by_self:
                        parts.append(f"你今天发起的交换请求:")
                        idx = 1
                        for r in swap_by_self:
                            parts.append(f"{idx}.向{r.target_user_name}交换老婆【{parse_wife_name(r.target_wife)}】")
                            idx += 1
                    if swap_by_other:
                        parts.append(f"你今天收到的交换请求:")
                        idx = 1
                        for r in swap_by_other:
                            parts.append(f"{idx}.{r.source_user_name}请求用【{parse_wife_name(r.source_wife)}】交换【{parse_wife_name(r.target_wife)}】")
                            idx += 1
                    await sql_conn.commit()
                    yield event.chain_result([Plain("\n".join(parts))])
                    return
        except Exception as e:
            logger.error(f"查看交换请求失败，{e}" + traceback.format_exc())
            yield event.chain_result([Plain("查看交换请求失败！请联系管理员")])

    @filter.command("清次数")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def clear_user_count(self, event: AstrMessageEvent):
        gid = str(event.message_obj.group_id)
        if not gid:
            yield event.chain_result("只能在群聊中使用")
            return
        tid, tname = parse_target_uid(event)
        if not tid:
            yield event.chain_result([Plain("请指定群员")])
            return
        today = get_today()
        try:
            async with aiosqlite.connect(SQLITE_FILE) as sql_conn:
                async with sql_conn.cursor() as cursor:
                    await UserCount.clear_count(cursor, gid, tid, today)
                    await sql_conn.commit()
                    yield event.chain_result([Plain(f"已清空{tname}的使用次数")])
                    return
        except Exception as e:
            logger.error(f"清空次数求失败，{e}" + traceback.format_exc())
            yield event.chain_result([Plain("清空次数失败！请联系管理员")])
