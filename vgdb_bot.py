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

import logging, psycopg2

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
    if update.effective_user.id == 165098508:
        with open('.pgdsn', encoding='utf-8') as dsnf:
            dsn = dsnf.read().replace('\n', '')
        conn = psycopg2.connect(dsn)
        with conn:
            cur = conn.cursor()
            sql = 'select pg_size_pretty(sum(size)) as Total_WAL_disk_usage from pg_ls_waldir();'
            try:
                cur.execute(sql)
                result = cur.fetchall()[0][0]
            except:
                pass
        conn.close()
        await update.message.reply_text(f"Synology WAL size is {result}")
    else:
        await update.message.reply_text('You do not have permission')

async def vacuum(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id == 165098508:
        with open('.pgdsn', encoding='utf-8') as dsnf:
            dsn = dsnf.read().replace('\n', '')
        conn = psycopg2.connect(dsn)
        conn.set_isolation_level(0)
        commands = ['vacuum (analyze) rfgf.license_blocks_rfgf;',
                    'vacuum (analyze) rfgf.rfgf_catalog;',
                    'analyze rfgf.license_blocks_rfgf;',
                    'analyze rfgf.rfgf_catalog;',
                    'REINDEX TABLE CONCURRENTLY rfgf.license_blocks_rfgf;',
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
        await update.message.reply_text('команды /vacuum выполнены')
        conn.close()
    else:
        await update.message.reply_text('You do not have permission')

def main() -> None:
    """Start the bot."""
    # Create the Application and pass it your bot's token.
    application = Application.builder().token("6702863648:AAHGC7f1wgw7IrAoGnk0CrOmQ_cU2Ag7LHM").build()

    # on different commands - answer in Telegram
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("wal", wal))
    application.add_handler(CommandHandler("menu", menu))
    application.add_handler(CommandHandler("vacuum", vacuum))

    # on non command i.e message - echo the message on Telegram
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, echo))

    # Run the bot until the user presses Ctrl-C
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()