from datetime import datetime
import requests
import pymsteams


def log_message(s, logf, bot_info, message, logdateformat='%Y-%m-%d %H:%M', to_telegram=True):
    logf.write(f"{datetime.now().strftime(logdateformat)} {message}\n")
    if to_telegram:
        send_to_telegram(s, logf, bot_info=bot_info, message=message, logdateformat=logdateformat)


def send_to_teams(webhook, message, logf, button_text='', button_link='', title='', sections=None):
    try:
        myTeamsMessage = pymsteams.connectorcard(webhook)
        myTeamsMessage.text(message)
        if button_text and button_link:
            myTeamsMessage.addLinkButton(button_text, button_link)
        if title:
            myTeamsMessage.title(title)
        if sections:
            if len(sections) > 10:
                logmessage = f"{datetime.now().strftime('%Y-%m-%d %H:%M')} Ошибка отправки сообщения в Teams. " \
                             f"Нельзя добавить больше 10 секций в карточку.\n"
                logf.write(logmessage)
                print(logmessage)
                return False
            else:
                sections_list = []
                for section_source in sections:
                    section = pymsteams.cardsection()
                    section.text(section_source)
                    sections_list.append(section)
                [myTeamsMessage.addSection(x) for x in sections_list]
        myTeamsMessage.send()
        return True
    except:
        logf.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M')} Ошибка отправки сообщения в Teams\n")


def send_to_telegram(s: requests.Session,
                     logf,
                     bot_info: tuple,
                     message='Hello vrom vgdb!',
                     logdateformat='%Y-%m-%d %H:%M:%S',
                     document='', 
                     photo=''):
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
    # create sendmessage url
    if document:
        telegram_url = f'https://api.telegram.org/bot{bot_token}/sendDocument'
    elif photo:
        telegram_url = f'https://api.telegram.org/bot{bot_token}/sendPhoto'
    else:
        telegram_url = f'https://api.telegram.org/bot{bot_token}/sendMessage'
    # start tries counter
    i = 1
    # try to send the message
    try:
        err_code = 0
        # res = s.post(telegram_url, json={'chat_id': bot_chatID, 'text': message})
        # if we receive an http error
        while err_code != 200 and i <= 10:
            # then try again, making 10 tries
            i += 1
            
            if document:
                with open(document, 'rb') as sf:
                    res = s.post(telegram_url,
                                 data={'chat_id': bot_chatID, 'caption': message},
                                #  data={'chat_id': bot_chatID, 'caption': message, 'parse_mode': 'MarkdownV2'},
                                 files={'document': sf}
                                 )
            elif photo:
                with open(photo, 'rb') as sf:
                    res = s.post(telegram_url,
                                 data={'chat_id': bot_chatID, 'caption': message},
                                #  data={'chat_id': bot_chatID, 'caption': message, 'parse_mode': 'MarkdownV2'},
                                 files={'photo': sf}
                                 )
            else:
                res = s.post(telegram_url, json={'chat_id': bot_chatID, 'text': message})
                # res = s.post(telegram_url, json={'chat_id': bot_chatID, 'text': message, 'parse_mode': 'MarkdownV2'})
                pass
            err_code = res.status_code
            reason = res.reason
            res_text = res.text
        if i > 10 and err_code != 200:
            # if 10 failed tries passed, send an error message to the log
            logf.write(
                f"{datetime.now().strftime(logdateformat)} 'Sending message from bot failed after 10 attempts, {err_code} error received, message=[{message}], reason=[{reason}], response_text={res_text}'\n")
            # and quit the loop
            print(f"{datetime.now().strftime(logdateformat)} 'Sending message from bot failed after 10 attempts, {err_code} error received, message=[{message}], reason=[{reason}], response_text={res_text}'\n")
            return False
    except:
        logf.write(
            f"{datetime.now().strftime(logdateformat)} 'Sending message from bot failed after {i} attempts, error sending request to telegram API. URL: [{telegram_url}]. Message: [{message}]'\n")
        print(f"{datetime.now().strftime(logdateformat)} 'Sending message from bot failed after {i} attempts, error sending request to telegram API. URL: [{telegram_url}]. Message: [{message}]'\n")
        return False
    return True