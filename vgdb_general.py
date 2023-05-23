from datetime import datetime
import requests


def send_to_telegram(s: requests.Session,
                     logf,
                     bot_info: tuple,
                     message='Hello vrom vgdb!',
                     logdateformat='%Y-%m-%d %H:%M:%S'):
    '''
    This function sends a message to a Telegram chat from a Telegram bot. \n
    You can create a bot using @BotFather. To obtain chat id you need to send a message to the bot, \n
    then go to https://api.telegram.org/bot<Bot Token>/getUpdates page and look for something like \n
    "chat":{"id":-1001814423962 ...}. The id parameter is the chat id. \n
    :param s: requests Session to send requests in;
    :param logf: text file object opened in 'w' mode to write log messages to;
    :param bot_info: tuple containing two strings: 1. telegram bot token; 2. telegram chat id to send message to;
    :param message: string containing the message;
    :param logdateformat: datetime format to use when writing log messages.
    :return: True if OK, False if not
    '''
    # get the bot token and the chat id from the input tuple
    bot_token = bot_info[0]
    bot_chatID = bot_info[1]
    # create senmessage url
    telegram_url = f'https://api.telegram.org/bot{bot_token}/sendMessage'
    # start tries counter
    i = 1
    # try to send the message
    try:
        res = s.post(telegram_url, json={'chat_id': bot_chatID, 'text': message})
        result = True
        # if we receive an http error
        while res.status_code != 200:
            # then try again, making 10 tries
            res = s.post(telegram_url, json={'chat_id': bot_chatID, 'text': message})
            i += 1
            if i > 10:
                # if 10 failed tries passed, send an error message to the log
                logf.write(
                    f"{datetime.now().strftime(logdateformat)} 'Sending message from bot failed after 10 attempts, {res.status_code} error received'\n")
                # and quit the loop
                result = False
                break
    except:
        logf.write(
            f"{datetime.now().strftime(logdateformat)} 'Sending message from bot failed after {i} attempts'\n")
        result = False
    return result