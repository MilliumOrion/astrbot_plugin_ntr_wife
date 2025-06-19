import aiosqlite
from typing import List

class Wife:
    def __init__(self, wife:str):
        self.wife = wife
    
    @classmethod
    async def init_table(cls, c: aiosqlite.Cursor,wife_list: List[str]):
        await c.execute(
            '''
    CREATE TABLE IF NOT EXISTS all_wife (
        wife TEXT NOT NULL PRIMARY KEY
    );
    '''
        )
        await c.executemany("INSERT OR IGNORE INTO all_wife (wife) VALUES (?)", [(name,) for name in wife_list])


    
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

    # 清空次数
    @classmethod
    async def clear_count(cls, c: aiosqlite.Cursor, gid: str, uid: str, day: int):
        await c.execute(
            """DELETE FROM user_count WHERE day=? AND gid=? AND uid=?""",
            (day, gid, uid),
        )


class UserWife:
    def __init__(self, gid: str, uid: str,  wife: str=""):
        self.gid = gid
        self.uid = uid
        self.wife = wife

    # 初始化老婆表
    @classmethod
    async def init_table(cls, c: aiosqlite.Cursor):
        await c.execute(
            '''
    CREATE TABLE IF NOT EXISTS wife (
        gid TEXT,
        uid TEXT,
        wife TEXT DEFAULT '',
        PRIMARY KEY (gid, uid )
    );
    '''
        )

    # 获取用户今天的老婆
    @classmethod
    async def get_user_wife(cls, c: aiosqlite.Cursor, gid: str, uid: str) -> 'UserWife':
        await c.execute(
            'SELECT wife FROM wife WHERE gid = ? AND uid = ?',
            (gid, uid),
        )
        value = await c.fetchone()
        if not value:
            return cls(gid=gid, uid=uid)
        return cls(gid=gid, uid=uid,  wife=value[0])

    # 保存用户今天的老婆
    async def save_user_wife(self, c: aiosqlite.Cursor) -> None:
        await c.execute(
            """INSERT INTO wife (gid,uid,wife)
                VALUES (?,?,?) 
                ON CONFLICT(gid,uid) DO UPDATE SET 
                wife = ?""",
            (self.gid, self.uid, self.wife, self.wife),
        )

    # 获取本群今天已经被抽取的老婆
    @classmethod
    async def get_random_wife(cls, c: aiosqlite.Cursor,  gid: str) ->str:
        await c.execute("""
SELECT a.wife
FROM all_wife a
LEFT JOIN wife w ON a.wife = w.wife AND w.gid = ?
WHERE w.wife IS NULL
ORDER BY RANDOM()
LIMIT 1;
""",(gid,))
        value =  await c.fetchone()
        if not value:
            return ""
        return value[0]

class SwapRequest:
    def __init__(
        self,
        gid: str,
        source_user: str,
        target_user: str,
        source_wife: str = "",
        target_wife: str = "",
        source_user_name: str = "",
        target_user_name: str = "",
    ):
        self.gid = gid
        self.source_user = source_user  # 发起者
        self.target_user = target_user  # 被还者
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
        source_wife TEXT DEFAULT '',
        target_wife TEXT DEFAULT '',
        source_user_name TEXT DEFAULT '',
        target_user_name TEXT DEFAULT '',
        PRIMARY KEY (gid, source_user, target_user)
    );
'''
        )


    # 保存用户今天交换的老婆
    async def save_request(
        self,
        c: aiosqlite.Cursor,
    ) -> None:
        await c.execute(
            """INSERT INTO swap_request (gid,source_user, target_user,  source_wife, target_wife,source_user_name, target_user_name)
                VALUES (?,?,?, ?, ?,?,?) 
                ON CONFLICT(gid,source_user, target_user) DO UPDATE SET 
                source_wife = ?, target_wife = ?, source_user_name = ?,target_user_name = ?""",
            (
                self.gid,
                self.source_user,
                self.target_user,
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
    async def delete_request(cls, c: aiosqlite.Cursor,  gid: str, source_user: str, target_user: str) -> None:
        await c.execute(
            'DELETE FROM swap_request WHERE gid=? AND source_user=? AND target_user=?',
            ( gid, source_user, target_user),
        )

    # 获取用户的交换请求
    @classmethod
    async def list_swap_request(cls, c: aiosqlite.Cursor,  gid: str, sid: str, tid: str) -> List['SwapRequest']:
        if tid:
            await c.execute(
                """SELECT 
                    gid,source_user, target_user, source_wife, target_wife,source_user_name, target_user_name
                FROM swap_request 
                WHERE gid=? AND target_user=?""",
                (gid, tid),
            )
        elif sid:
            await c.execute(
                """SELECT 
                    gid,source_user, target_user, source_wife, target_wife,source_user_name, target_user_name
            FROM swap_request 
            WHERE gid=? AND source_user=?""",
                (gid, sid),
            )
        else:
            return []
        return [
            cls(
                gid=row[0],
                source_user=row[1],
                target_user=row[2],
                source_wife=row[3],
                target_wife=row[4],
                source_user_name=row[5],
                target_user_name=row[6],
            )
            for row in await c.fetchall()
        ]

    @classmethod
    async def get(cls, c: aiosqlite.Cursor, gid: str, sid: str, tid: str):
        await c.execute(
            """SELECT 
                gid,source_user, target_user,source_wife, target_wife,source_user_name, target_user_name
            FROM swap_request 
            WHERE gid=? AND source_user=? AND target_user=? """,
            (gid, sid, tid),
        )
        row = await c.fetchone()
        return cls(
            gid=row[0],
            source_user=row[1],
            target_user=row[2],
            source_wife=row[3],
            target_wife=row[4],
            source_user_name=row[5],
            target_user_name=row[6],
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
