from vgdb_rapid_scada import *


with open('.scadadsn', 'r', encoding='utf-8') as f:
    scada_login = json.load(f)
with open('.pgdsn', encoding='utf-8') as f:
    pgdsn = f.read()

data = load_from_scada([('Интинская-18', 'Скважина', ['101-108'])], scada_login)
if data:
    channels_dict = {
        "101": "Давление трубное",
        "102": "Температура газа",
        "103": "Давление затрубное",
        "104": "Давление газа на входе сепаратора (после штуцера)",
        "105": "Температура газа на входе сепаратора (после штуцера)",
        "106": "Давление в дренажной линии сепаратора",
        "107": "Уровень в сепараторе",
        "108": "Test1"
    }
    send_to_postgres(pgdsn, 'culture.from_scada', data, channels_dict)