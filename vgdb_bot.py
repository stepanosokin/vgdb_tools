# https://docs.python-telegram-bot.org/en/v20.6/examples.html
# https://docs.python-telegram-bot.org/en/v20.6/examples.echobot.html

#!/usr/bin/env python
# pylint: disable=unused-argument
# This program is dedicated to the public domain under the CC0 license.

"""
Simple Bot to reply to Telegram messages.

First, a few handler functions are defined. Then, those functions are passed to
the Application and registered at their respective places.
Then, the bot is started and runs until we press Ctrl-C on the command line.

Usage:
Basic Echobot example, repeats messages.
Press Ctrl-C on the command line or send a signal to the process to stop the
bot.
"""

import logging, psycopg2, json, io
from psycopg2.extras import DictCursor
from vgdb_torgi_gov_ru import *
import vgdb_license_blocks_rfgf
from synchro_evergis import *
from vgdb_auctions_rosnedra import *

from telegram import ForceReply, Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

# Enable logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
# set higher logging level for httpx to avoid all GET and POST requests being logged
logging.getLogger("httpx").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


# Define a few command handlers. These usually take the two arguments update and
# context.
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send a message when the command /start is issued."""
    user = update.effective_user
    await update.message.reply_html(
        rf"Hi {user.mention_html()}!",
        reply_markup=ForceReply(selective=True, input_field_placeholder='This is my placeholder'),
    )
    print(user.first_name)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send a message when the command /help is issued."""
    await update.message.reply_text("Help!")


async def echo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Echo the user message."""
    await update.message.reply_text(update.message.text)


async def menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text('/wal - check WAL size on Synology\n'
                                    '/vacuum - vacuum, analyze and reindex biggest tables')


async def wal(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id in [165098508, 165635882]:
        with open('.pgdsn', encoding='utf-8') as dsnf:
            dsn = dsnf.read().replace('\n', '')
        conn = psycopg2.connect(dsn)
        with conn:
            cur = conn.cursor(cursor_factory=DictCursor)
            sql = 'select pg_size_pretty(sum(size)) as Total_WAL_disk_usage from pg_ls_waldir();'
            message = ''
            try:
                cur.execute(sql)
                walsize = cur.fetchall()[0]['total_wal_disk_usage']
                message += f"WAL size: {walsize}"
                sql = 'select pg_current_wal_lsn();'
                cur.execute(sql)
                wal_lsn = cur.fetchall()[0]['pg_current_wal_lsn']
                message += f"\nCurrent wal lsn: {wal_lsn}"
                sql = 'select * from pg_stat_replication;'
                cur.execute(sql)
                pg_stat_rep = cur.fetchall()
                if pg_stat_rep:
                    message += f"\n----------\nReplication stat:"
                    for rep in pg_stat_rep:
                        message += f"\n{rep['application_name']}:" \
                                   f"\n{rep['sent_lsn']} -> {rep['write_lsn']}" \
                                   f"\nlag: {rep['write_lag']}"
            except:
                pass
        conn.close()
        # await update.message.reply_text(f"Synology WAL size is {walsize}")
        await update.message.reply_text(message)
    else:
        await update.message.reply_text('You do not have permission')


async def jerks(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    secs = datetime.now().isocalendar().week * 10 + 140
    if secs > 240:
        secs = 240
    message = f'Зарядка на сегодня:\n\n' \
              f'Приседания:\nhttps://youtu.be/6A2V9Bu80J4?si=h-UUwQpZ1KRo4d7Q\n\n' \
              f'Планка: {str(secs // 60)}:{str(secs % 60)}'
    await update.message.reply_text(message)


async def vacuum(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id == 165098508:
        with open('.pgdsn', encoding='utf-8') as dsnf:
            dsn = dsnf.read().replace('\n', '')
        conn = psycopg2.connect(dsn)
        conn.set_isolation_level(0)
        # commands = ['vacuum (analyze) rfgf.license_blocks_rfgf;',
        #             'vacuum (analyze) rfgf.rfgf_catalog;',
        #             'analyze rfgf.license_blocks_rfgf;',
        #             'analyze rfgf.rfgf_catalog;',
        #             'REINDEX TABLE CONCURRENTLY rfgf.license_blocks_rfgf;',
        #             'REINDEX TABLE CONCURRENTLY rfgf.rfgf_catalog;']
        commands = ['vacuum;']
        for sql in commands:
            with conn.cursor() as cur:
                try:
                    conn.commit()
                    conn.set_isolation_level(0)
                    cur.execute(sql)
                    await update.message.reply_text(f"{cur.statusmessage}")
                except:
                    await update.message.reply_text(f"Error query '{sql}': {cur.connection.info.error_message}")
        await update.message.reply_text('команды /vacuum выполнены')
        conn.close()
    else:
        await update.message.reply_text('You do not have permission')


async def analyze(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id == 165098508:
        with open('.pgdsn', encoding='utf-8') as dsnf:
            dsn = dsnf.read().replace('\n', '')
        conn = psycopg2.connect(dsn)
        conn.set_isolation_level(0)
        commands = ['analyze rfgf.license_blocks_rfgf;',
                    'analyze rfgf.rfgf_catalog;']
        for sql in commands:
            with conn.cursor() as cur:
                try:
                    conn.commit()
                    conn.set_isolation_level(0)
                    cur.execute(sql)
                    await update.message.reply_text(f"{cur.statusmessage}")
                except:
                    await update.message.reply_text(f"Error query '{sql}': {cur.connection.info.error_message}")
        await update.message.reply_text('команды /analyze выполнены')
        conn.close()
    else:
        await update.message.reply_text('You do not have permission')


async def reindex(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id == 165098508:
        with open('.pgdsn', encoding='utf-8') as dsnf:
            dsn = dsnf.read().replace('\n', '')
        conn = psycopg2.connect(dsn)
        conn.set_isolation_level(0)
        commands = ['REINDEX TABLE CONCURRENTLY rfgf.license_blocks_rfgf;',
                    'REINDEX TABLE CONCURRENTLY rfgf.rfgf_catalog;']
        for sql in commands:
            with conn.cursor() as cur:
                try:
                    conn.commit()
                    conn.set_isolation_level(0)
                    cur.execute(sql)
                    await update.message.reply_text(f"{cur.statusmessage}")
                except:
                    await update.message.reply_text(f"Error query '{sql}': {cur.connection.info.error_message}")
        await update.message.reply_text('команды /reindex выполнены')
        conn.close()
    else:
        await update.message.reply_text('You do not have permission')


async def torgi(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id == 165098508:
        with open('.pgdsn', encoding='utf-8') as dsnf:
            dsn = dsnf.read().replace('\n', '')
        with open('bot_info_vgdb_bot_toAucGroup.json', 'r', encoding='utf-8') as f:
            jdata = json.load(f)
            report_bot_info = (jdata['token'], jdata['chatid'])
        with open('bot_info_vgdb_bot_toStepan.json', 'r', encoding='utf-8') as f:
            jdata = json.load(f)
            log_bot_info = (jdata['token'], jdata['chatid'])
        with open('2023_blocks_nr_ne.webhook', 'r', encoding='utf-8') as f:
            nr_ne_webhook_2023 = f.read().replace('\n', '')
        with open('.egssh', 'r', encoding='utf-8') as f:
            egssh = json.load(f)
        refresh_lotcards(dsn=dsn, log_bot_info=log_bot_info, report_bot_info=report_bot_info,
                         webhook=nr_ne_webhook_2023)
        synchro_table([('torgi_gov_ru', ['lotcards'])], '.pgdsn', '.ext_pgdsn',
                      ssh_host=egssh["host"], ssh_user=egssh["user"], bot_info=log_bot_info)
        await update.message.reply_text('команда /torgi выполнена')
    else:
        await update.message.reply_text('You do not have permission')


async def lic(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id == 165098508:
        with open('.pgdsn', encoding='utf-8') as f:
            local_pgdsn = f.read()
        with open('.ext_pgdsn', encoding='utf-8') as f:
            ext_pgdsn = f.read()
        with open('bot_info_vgdb_bot_toStepan.json', 'r', encoding='utf-8') as f:
            jdata = json.load(f)
            bot_info = (jdata['token'], jdata['chatid'])
        with open('.pggdal', encoding='utf-8') as gdalf:
            gdalpgcs = gdalf.read().replace('\n', '')
        with open('license_blocks_general.webhook', 'r', encoding='utf-8') as f:
            lb_general_webhook = f.read().replace('\n', '')
        with open('.egssh', 'r', encoding='utf-8') as f:
            egssh = json.load(f)
        # download the license blocks data from Rosgeolfond
        if vgdb_license_blocks_rfgf.download_rfgf_blocks('rfgf_request_noFilter_300000.json', 'rfgf_result_300000.json', bot_info=bot_info):
            # parse the blocks from downloaded json
            if vgdb_license_blocks_rfgf.parse_rfgf_blocks('rfgf_result_300000.json', bot_info=bot_info):
                # update license blocks on server
                if vgdb_license_blocks_rfgf.update_postgres_table(gdalpgcs, bot_info=bot_info, webhook=lb_general_webhook):
                    synchro_layer([('rfgf', ['license_blocks_rfgf'])], local_pgdsn, ext_pgdsn,
                                  ssh_host=egssh["host"], ssh_user=egssh["user"], bot_info=bot_info)
        await update.message.reply_text('команда /lic выполнена')
    else:
        await update.message.reply_text('You do not have permission')


async def rosnedra(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id == 165098508:
        with open('.pgdsn', encoding='utf-8') as f:
            local_pgdsn = f.read()
        with open('.ext_pgdsn', encoding='utf-8') as f:
            ext_pgdsn = f.read()
        with open('bot_info_vgdb_bot_toAucGroup.json', 'r', encoding='utf-8') as f:
            jdata = json.load(f)
            report_bot_info = (jdata['token'], jdata['chatid'])
        with open('bot_info_vgdb_bot_toStepan.json', 'r', encoding='utf-8') as f:
            jdata = json.load(f)
            bot_info = (jdata['token'], jdata['chatid'])
        with open('.pggdal', encoding='utf-8') as gdalf:
            gdalpgcs = gdalf.read().replace('\n', '')
        with open('license_blocks_general.webhook', 'r', encoding='utf-8') as f:
            lb_general_webhook = f.read().replace('\n', '')
        with open('2024_blocks_nr_ne.webhook', 'r', encoding='utf-8') as f:
            blocks_nr_ne_webhook = f.read().replace('\n', '')
        with open('2024_blocks_np.webhook', 'r', encoding='utf-8') as f:
            blocks_np_webhook = f.read().replace('\n', '')

        with open('.egssh', 'r', encoding='utf-8') as f:
            egssh = json.load(f)

        pgconn = psycopg2.connect(local_pgdsn)
        lastdt_result = get_latest_order_date_from_synology(pgconn)
        if lastdt_result[0]:
            startdt = lastdt_result[1] + timedelta(days=1)
            clear_folder('rosnedra_auc')
            if download_orders(start=startdt, end=datetime.now(), search_string='Об утверждении Перечня участков недр',
                               folder='rosnedra_auc', bot_info=bot_info):
                if parse_blocks_from_orders(folder='rosnedra_auc', gpkg='rosnedra_result.gpkg',
                                            bot_info=bot_info, report_bot_info=report_bot_info,
                                            blocks_np_webhook=blocks_np_webhook,
                                            blocks_nr_ne_webhook=blocks_nr_ne_webhook,
                                            pgconn=pgconn):
                    if update_postgres_table(gdalpgcs, folder='rosnedra_auc', bot_info=bot_info):
                        synchro_layer([('rosnedra', ['license_blocks_rosnedra_orders'])], local_pgdsn, ext_pgdsn
                                      , ssh_host=egssh["host"], ssh_user=egssh["user"],
                                      bot_info=bot_info)
            pgconn.close()
        await update.message.reply_text('команда /rosnedra выполнена')
    else:
        await update.message.reply_text('You do not have permission')


async def get(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if context.args:
        if context.args[0] == 'license':
            if len(context.args) == 2:
                with open('.pgdsn', encoding='utf-8') as dsnf:
                    dsn = dsnf.read().replace('\n', '')
                conn = psycopg2.connect(dsn)
                if ';' not in context.args[1]:
                    searchstring = context.args[1].lower().replace(chr(27), '').replace(chr(22), '').lower()
                    sql = f"select json_build_object('type', 'FeatureCollection', " \
                          f"'features', json_agg(st_asgeojson(t.*)::json)) FROM (" \
                          f"select * from rfgf.license_blocks_rfgf_hc_active " \
                          f"where LOWER(license_block_name) like '%{searchstring}%' " \
                          f"or LOWER(gos_reg_num) like '%{searchstring}%' " \
                          f"or LOWER(user_info) like '%{searchstring}%' " \
                          f"limit 10) as t;"
                    with conn:
                        with conn.cursor() as cur:
                            cur.execute(sql)
                            result = cur.fetchall()[0][0]
                            pass
                            # await update.message.reply_document(json.dump(result, ensure_ascii=False))
                            # await update.message.reply_document(io.StringIO(json.dumps(result)), caption='result')
                            with open('Лицензионные участки.json', 'w') as rfile:
                                json.dump(result, rfile, ensure_ascii=False)
                            with open('Лицензионные участки.json', 'rb') as sfile:
                                await update.message.reply_document(sfile, caption=context.args[1])
                else:
                    await update.message.reply_text('Запрещенные символы')
            else:
                await update.message.reply_text('Укажите часть названия участка как второй параметр')
        else:
            await update.message.reply_text('Укажите корректный параметр.')
    else:
        await update.message.reply_text('Укажите параметры команды:\nlicense - лицензионные участки в geojson (макс.10)')


def main() -> None:
    """Start the bot."""
    # Create the Application and pass it your bot's token.
    with open('bot_info_vgdb_bot_toStepan.json', 'r', encoding='utf-8') as f:
        jdata = json.load(f)
    application = Application.builder().token(jdata['token']).build()

    # on different commands - answer in Telegram
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("wal", wal))
    application.add_handler(CommandHandler("menu", menu))
    application.add_handler(CommandHandler("vacuum", vacuum))
    application.add_handler(CommandHandler("analyze", analyze))
    application.add_handler(CommandHandler("reindex", reindex))
    application.add_handler(CommandHandler("torgi", torgi))
    application.add_handler(CommandHandler("rosnedra", rosnedra))
    application.add_handler(CommandHandler("lic", lic))
    application.add_handler(CommandHandler("get", get))
    application.add_handler(CommandHandler("jerks", jerks))

    # on non command i.e message - echo the message on Telegram
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, echo))

    # Run the bot until the user presses Ctrl-C
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()