from vgdb_rapid_scada import *
from synchro_evergis import *


with open('.scadadsn', 'r', encoding='utf-8') as f:
    scada_login = json.load(f)
with open('.pgdsn', encoding='utf-8') as f:
    pgdsn = f.read()
with open('bot_info_vgdb_bot_toStepan.json', 'r', encoding='utf-8') as f:
    jdata = json.load(f)
    bot_info = (jdata['token'], jdata['chatid'])

# data = load_from_scada([('Интинская-18', 'Скважина', ['101-108'])], scada_login, bot_info=bot_info)
# if data:
#     channels_dict = {
#         "101": "Давление трубное",
#         "102": "Температура газа",
#         "103": "Давление затрубное",
#         "104": "Давление газа на входе сепаратора (после штуцера)",
#         "105": "Температура газа на входе сепаратора (после штуцера)",
#         "106": "Давление в дренажной линии сепаратора",
#         "107": "Уровень в сепараторе",
#         "108": "Test1"
#     }
#     if send_to_postgres(pgdsn, 'culture.from_scada', data, channels_dict, bot_info=bot_info):
#         synchro_table([('culture', ['from_scada'])], '.pgdsn', '.ext_pgdsn', bot_info=bot_info)

data = load_from_scada([('Интинская-18', 'ДЭЛ-150', ['110-123'])], scada_login, bot_info=bot_info)
if data:
    channels_dict = {
        "110": "Нагрузка на крюк [тс]",
        "111": "Давление нагнетания ЦА [кгс/см2]",
        "112": "ПЖ ВХ ДАВЛ 2 [кгс/см2]",
        "113": "ПЖ ВХ ДАВЛ 3 [кгс/см2]",
        "114": "ГК МОМЕНТ [кгс*м]",
        "115": "ГК ДАВЛЕНИЕ [кгс/см2]",
        "116": "ПЛОТН ПЖ 1 [г/см3]",
        "117": "ПЛОТН ПЖ 2 [г/см3]",
        "118": "ПЖ УРОВ 1 [м3]",
        "119": "ПЖ УРОВ 2 [м3]",
        "120": "CH4_1 [% НКПР]",
        "121": "H2S_2 [мг/м3]",
        "122": "CH4_3 [% НКПР]",
        "123": "H2S_4 [мг/м3]"
    }
    if send_to_postgres(pgdsn, 'culture.from_scada', data, channels_dict, bot_info=bot_info):
        synchro_table([('culture', ['from_scada'])], '.pgdsn', '.ext_pgdsn', bot_info=bot_info)