import datetime

import db
import sqlalchemy

def assing_presence(con: sqlalchemy.engine.Engine, nick_user:str):
    date = datetime.datetime.now().strftime("%Y-%m-%d")
    sql = "INSERT INTO tb_presence (dtPresence, descNick) VALUES (?,?)"
    con.execute(sql, (date, nick_user))
    return True

def check_assing_presence(con: sqlalchemy.engine.Engine, nick_user:str):
    date = datetime.datetime.now().strftime("%Y-%m-%d")
    sql = f"""SELECT COUNT(*)
              FROM tb_presence
              WHERE dtPresence = '{date}'
              AND descNick = '{nick_user}'"""
    exec = con.execute(sql)
    return exec.fetchone()[0] > 0
