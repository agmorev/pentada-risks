import firebirdsql as fdb
from sshtunnel import SSHTunnelForwarder
import pandas as pd
import telebot
import requests


# while True:

ip = requests.get("https://api.ipify.org?format=json").json()["ip"]
print(ip)


# GET DATA FROM DATABASE
server = SSHTunnelForwarder(
        ('mail.pentada-brok.com', 57300),
        ssh_username="agmorev",
        ssh_password="Jur48dl§hfi!83",
        remote_bind_address=('192.168.70.99', 3051),
        local_bind_address=('127.0.0.1', 3050)
        )
server.start()
print(server)

con = fdb.connect(
        host='127.0.0.1',
        database='C:/MasterD/MDGarant/PentadaDB/Db/MDGARANT.FDB',
        port=3050,
        user='SYSDBA',
        password='masterkey',
        charset='UTF8'
)

print(con.db_info(fdb.isc_info_user_names))

cur = con.cursor()
ex = cur.execute('''
                SELECT
                    IDENT,
                    GL_NUM,
                    GL_DATE,
                    GL_CL_NAME,
                    GL_CL_OKPO,
                    GL_CAR_NAME,
                    GL_CAR_ADR,
                    GL_SUMMA
                FROM GARANT_REQ
                ORDER BY IDENT DESC ROWS 1;
                '''
)
print(ex.fetchall())

event = con.event_conduit(['get_new_order'])
print('HANDLER: About to wait for the occurrence of one of "new order"...\n')
result = event.wait()
print('HANDLER: An event notification has arrived:')
print(result)
event.close()
df = pd.read_sql('''
                SELECT
                IDENT,
                GL_NUM,
                GL_DATE,
                GL_CL_NAME,
                GL_CL_OKPO,
                GL_CAR_NAME,
                GL_CAR_ADR,
                GL_SUMMA
                FROM GARANT_REQ
                ORDER BY IDENT DESC ROWS 1;
                ''', con)
print(df)
con.close()
server.stop()

####################################################
###################RISKS PROFILES###################
####################################################

w_number = df.loc[0, 'GL_NUM']
w_date = df.loc[0, 'GL_DATE']
w_client = df.loc[0, 'GL_CL_NAME']
w_carrier = df.loc[0, 'GL_CAR_NAME']
w_sum = df.loc[0, 'GL_SUMMA']

#RISK PROFILE #1 - ЗА СУМОЮ ГАРАНТІЇ, ЩО ПЕРЕВИЩУЄ 1.2 МЛН.ГРН.
if w_sum >= 1200000.00:
        # Create bot
        bot = telebot.TeleBot(token='1173390946:AAFd65-o35punvwx7NXT92HtR_-o5a9aTms')
        bot.send_message(1061732281, '‼️‼️‼️ *РИЗИКИ* ‼️‼️‼️', parse_mode='Markdown')

        total_sum = "{:,.2f}".format(w_sum)

        bot.send_message(
                1061732281,
                '''🪤 Заявка №{} від {}\n-клієнт - *{}*\n-перевізник - *{}*\n-сума - *{}* грн.'''.format(w_number, w_date, w_client, w_carrier, total_sum),
                parse_mode='Markdown')
        bot.send_message(
                1061732281,
                '''🛑 РИЗИК 1️⃣\nСума митних платежів, яка підлягає гарантуванню, дорівнює або превищує *1.2 млн.грн.*\n-----------------------------\n*🟢 ЗАХОДИ:*\n‼️ ️Оформлення гарантії необхідно попередньо узгодити з керівництвом''',
                parse_mode='Markdown')








