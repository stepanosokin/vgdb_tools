import locale

print(locale.getlocale())
locale.setlocale(locale.LC_ALL, 'ru_RU.UTF-8')
print(locale.getlocale())
locale.setlocale(locale.LC_ALL, '')
print(locale.getlocale())