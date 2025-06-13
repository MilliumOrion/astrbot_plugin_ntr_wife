import aiosqlite
from typing import List


class UserCount:
    def __init__(self, gid: str, uid: str, day: int, ntr_count: int = 0, swap_count: int = 0, change_count: int = 0):
        self.gid = gid
        self.uid = uid
        self.ntr_count = ntr_count
        self.swap_count = swap_count
        self.change_count = change_count
        self.day = day

    @classmethod
    async def init_table(cls, c: aiosqlite.Cursor):
        await c.execute(
            '''
    CREATE TABLE IF NOT EXISTS user_count (
        day INTEGER,
        gid TEXT,
        uid TEXT,
        ntr_count INTEGER DEFAULT 0,
        swap_count INTEGER DEFAULT 0,
        change_count INTEGER DEFAULT 0,
        PRIMARY KEY (day,gid,uid)
    );
    '''
        )

    # 清理过期的计数数据
    @classmethod
    async def clear_expired(cls, c: aiosqlite.Cursor, today: int) -> None:
        await c.execute(
            """DELETE FROM user_count WHERE day < ?""",
            (today,),
        )

    # 获取今天ntr、交换、换老婆次数
    @classmethod
    async def get_count(cls, c: aiosqlite.Cursor, gid: str, uid: str, day: int) -> 'UserCount':
        await c.execute('SELECT ntr_count,swap_count,change_count FROM user_count WHERE day=? AND gid=? AND uid=?', (day, gid, uid))
        value = await c.fetchone()
        if not value:
            return cls(gid=gid, uid=uid, day=day)
        return cls(gid=gid, uid=uid, day=day, ntr_count=value[0], swap_count=value[1], change_count=value[2])

    # 新增今天ntr次数
    @classmethod
    async def increase_count(cls,c: aiosqlite.Cursor, gid: str, uid: str, day: int, field: str):
        await c.execute(
            f"""INSERT INTO user_count (day,gid,uid,{field}) 
                VALUES (?,?,?,1) 
                ON CONFLICT(day,gid,uid) DO UPDATE SET 
                {field} = {field} + 1""",
            (day, gid, uid),
        )


class UserWife:
    def __init__(self, gid: str, uid: str, day: int, wife: str=""):
        self.gid = gid
        self.uid = uid
        self.day = day
        self.wife = wife

    # 初始化老婆表
    @classmethod
    async def init_table(cls, c: aiosqlite.Cursor):
        await c.execute(
            '''
    CREATE TABLE IF NOT EXISTS wife (
        day INTEGER,
        gid TEXT,
        uid TEXT,
        wife TEXT DEFAULT '',
        PRIMARY KEY (day,gid, uid )
    );
    '''
        )

    # 清理过期的老婆数据
    @classmethod
    async def clear_expired(cls, c: aiosqlite.Cursor, today: int) -> None:
        await c.execute(
            """DELETE FROM wife WHERE day < ?""",
            (today,),
        )

    # 获取用户今天的老婆
    @classmethod
    async def get_user_wife(cls, c: aiosqlite.Cursor, gid: str, uid: str, day: int) -> 'UserWife':
        await c.execute(
            'SELECT wife FROM wife WHERE day = ? AND gid = ? AND uid = ?',
            (day, gid, uid),
        )
        value = await c.fetchone()
        if not value:
            return cls(gid=gid, uid=uid, day=day)
        return cls(gid=gid, uid=uid, day=day, wife=value[0])

    # 保存用户今天的老婆
    async def save_user_wife(self, c: aiosqlite.Cursor) -> None:
        await c.execute(
            """INSERT INTO wife (day,gid,uid,wife)
                VALUES (?,?,?,?) 
                ON CONFLICT(day,gid,uid) DO UPDATE SET 
                wife = ?""",
            (self.day, self.gid, self.uid, self.wife, self.wife),
        )

    # 获取本群今天已经被抽取的老婆
    @classmethod
    async def get_group_used_wife(cls, c: aiosqlite.Cursor,  gid: str,day: int) -> List[str]:
        await c.execute(
            """SELECT wife FROM wife WHERE day=? and gid=?""",(day, gid),
        )
        return [row[0] for row in await c.fetchall()]


class SwapRequest:
    def __init__(
        self,
        gid: str,
        source_user: str,
        target_user: str,
        day: int,
        source_wife: str = "",
        target_wife: str = "",
        source_user_name: str = "",
        target_user_name: str = "",
    ):
        self.gid = gid
        self.source_user = source_user  # 发起者
        self.target_user = target_user  # 被还者
        self.day = day
        self.source_wife = source_wife  # 发起者的老婆
        self.target_wife = target_wife  # 被还者的老婆
        self.source_user_name = source_user_name  # 发起者
        self.target_user_name = target_user_name  # 被还者

    @classmethod
    async def init_table(cls, c: aiosqlite.Cursor):
        await c.execute(
            '''
    CREATE TABLE IF NOT EXISTS swap_request (
        gid TEXT,
        source_user TEXT,
        target_user TEXT,
        day INTEGER,
        source_wife TEXT DEFAULT '',
        target_wife TEXT DEFAULT '',
        source_user_name TEXT DEFAULT '',
        target_user_name TEXT DEFAULT '',
        PRIMARY KEY (day,gid, source_user, target_user)
    );
'''
        )

    # 清理过期的老婆数据
    @classmethod
    async def clear_expired(cls, c: aiosqlite.Cursor, today: int) -> None:
        await c.execute(
            """DELETE FROM swap_request WHERE day < ?""",
            (today,),
        )

    # 保存用户今天交换的老婆
    async def save_request(
        self,
        c: aiosqlite.Cursor,
    ) -> None:
        await c.execute(
            """INSERT INTO swap_request (gid,source_user, target_user, day, source_wife, target_wife,source_user_name, target_user_name)
                VALUES (?,?,?, ?, ?, ?,?,?) 
                ON CONFLICT(day,gid,source_user, target_user) DO UPDATE SET 
                source_wife = ?, target_wife = ?, source_user_name = ?,target_user_name = ?""",
            (
                self.gid,
                self.source_user,
                self.target_user,
                self.day,
                self.source_wife,
                self.target_wife,
                self.source_user_name,
                self.target_user_name,
                self.source_wife,
                self.target_wife,
                self.source_user_name,
                self.target_user_name,
            ),
        )

    # 删除指定双方的交换记录
    @classmethod
    async def delete_request(cls, c: aiosqlite.Cursor,  gid: str, source_user: str, target_user: str,day: int) -> None:
        await c.execute(
            'DELETE FROM swap_request WHERE day=? AND gid=? AND source_user=? AND target_user=?',
            (day, gid, source_user, target_user),
        )

    # 获取用户的交换请求
    @classmethod
    async def list_swap_request(cls, c: aiosqlite.Cursor,  gid: str, sid: str, tid: str,day: int) -> List['SwapRequest']:
        if tid:
            await c.execute(
                """SELECT 
                    gid,source_user, target_user, day, source_wife, target_wife,source_user_name, target_user_name
                FROM swap_request 
                WHERE day=? AND gid=? AND target_user=?""",
                (day, gid, tid),
            )
        elif sid:
            await c.execute(
                """SELECT 
                    gid,source_user, target_user, day, source_wife, target_wife,source_user_name, target_user_name
            FROM swap_request 
            WHERE day=? AND gid=? AND source_user=?""",
                (day, gid, sid),
            )
        else:
            return []
        return [
            cls(
                gid=row[0],
                source_user=row[1],
                target_user=row[2],
                day=row[3],
                source_wife=row[4],
                target_wife=row[5],
                source_user_name=row[6],
                target_user_name=row[7],
            )
            for row in await c.fetchall()
        ]

    @classmethod
    async def get(cls, c: aiosqlite.Cursor, gid: str, sid: str, tid: str, day: str):
        await c.execute(
            """SELECT 
                gid,source_user, target_user, day, source_wife, target_wife,source_user_name, target_user_name
            FROM swap_request 
            WHERE day=? AND gid=? AND source_user=? AND target_user=? """,
            (day, gid, sid, tid),
        )
        row = await c.fetchone()
        return cls(
            gid=row[0],
            source_user=row[1],
            target_user=row[2],
            day=row[3],
            source_wife=row[4],
            target_wife=row[5],
            source_user_name=row[6],
            target_user_name=row[7],
        )


class GroupConfig:

    @classmethod
    async def init_table(cls,c: aiosqlite.Cursor):
        await c.execute(
            '''
    CREATE TABLE IF NOT EXISTS group_config (
        gid TEXT,
        enable_ntr INTEGER DEFAULT 0,
        PRIMARY KEY (gid)
    );
    '''
        )

    # 群是否开启牛老婆功能
    @classmethod
    async def is_group_ntr_enable(cls, c: aiosqlite.Cursor, gid: str):
        await c.execute('SELECT enable_ntr FROM group_config WHERE gid=?', (gid,))
        value = await c.fetchone()
        if not value:
            return False
        return True if value[0] == 1 else False

    # 设置群是否开启nt
    @classmethod
    async def set_group_ntr(cls, c: aiosqlite.Cursor, gid: str, enable: int) -> None:
        await c.execute(
            """INSERT INTO group_config (gid,enable_ntr)
                VALUES (?,?) 
                ON CONFLICT(gid) DO UPDATE SET 
                enable_ntr = ?""",
            (gid, enable, enable),
        )


class UserWifeHisotry:
    def __init__(self, uid: str, wife_name: str):
        self.uid = uid
        self.wife_name = wife_name

    # 初始化老婆表
    @classmethod
    async def init_table(cls, c: aiosqlite.Cursor):
        await c.execute(
            '''
    CREATE TABLE IF NOT EXISTS user_wife_history (
        uid TEXT,
        wife_name TEXT,
        PRIMARY KEY (uid,wife_name )
    );
    '''
        )

    # 添加老婆历史
    @classmethod
    async def add_wife_histroy(cls,c: aiosqlite.Cursor, uid: str, wife_name: str) -> bool:
        await c.execute(
            '''
        INSERT OR IGNORE INTO user_wife_history (uid, wife_name) VALUES (?, ?);
        ''',
            (uid, wife_name),
        )

        # 检查插入的行数
        if c.rowcount == 0:
            return False
        else:
            return True


class WifeCount:
    # 初始化
    @classmethod
    async def init_table(cls, c: aiosqlite.Cursor):
        await c.execute(
            '''
    CREATE TABLE IF NOT EXISTS wife_count (
        gid TEXT,
        wife_name TEXT,
        draw_count INTEGER DEFAULT 0,
        ntr_count INTEGER DEFAULT 0,
        swap_count INTEGER DEFAULT 0,
        divorce_count INTEGER DEFAULT 0,
        PRIMARY KEY (gid,wife_name )
    );
    '''
        )

    # 添加老婆的数据统计
    @classmethod
    async def increase_count(cls, c: aiosqlite.Cursor, gid: str, wife_name: str, field: str):
        await c.execute(
            f"""INSERT INTO wife_count (gid,wife_name,{field}) 
                VALUES (?,?,1) 
                ON CONFLICT(gid,wife_name) DO UPDATE SET 
                {field} = {field} + 1
                RETURNING 'updated' AS action
                """,
            (gid, wife_name),
        )
        result = await c.fetchone()
        # 检查插入的行数
        if result:
            return False
        else:
            return True
