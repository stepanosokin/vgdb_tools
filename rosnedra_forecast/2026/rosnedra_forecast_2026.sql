CREATE TABLE IF NOT EXISTS rosnedra.blocks_hcs_forecast
(
    gid SERIAL PRIMARY KEY,
    year integer,
    datestamp date,
    region text,
    block_name text,
    source text,
    url text,
    resources_raw text,
    resources_parsed json
);
insert into rosnedra.blocks_hcs_forecast(year, datestamp, region, block_name, source, url, resources_raw, resources_parsed) values(2026, '2026-01-02', 'Волгоградская область', e'Малодельский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'газ (извл.):
C1 - 0,176 млрд м3,
C2 - 0,071 млрд м3', '{"gas": {"C1": [0.176, "млрд м3"], "C2": [0.071, "млрд м3"]}}'),
(2026, '2026-01-02', 'Волгоградская область', e'Антиповско-Лебяжинский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
C1 - 0,018 млн т
газ (извл.):
C1 - 0,196 млрд м3', '{"oil": {"C1": [0.018, "млн т"]}, "gas": {"C1": [0.196, "млрд м3"]}}'),
(2026, '2026-01-02', 'Иркутская область', e'Восточно-Ордынский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
D1 - 0,2 млн т,
D2 - 1,2 млн т
газ (извл.):
D1 - 42,2 млрд м3,
D2 - 11,6 млрд м3
конденсат (извл.):
D1 - 2,5 млн т,
D2 - 0,7 млн т', '{"oil": {"D1": [0.2, "млн т"], "D2": [1.2, "млн т"]}, "gas": {"D1": [42.2, "млрд м3"], "D2": [11.6, "млрд м3"]}, "cond": {"D1": [2.5, "млн т"], "D2": [0.7, "млн т"]}}'),
(2026, '2026-01-02', 'Краснодарский край', e'Кеслеровское', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
A - 7,00 млн т,
D1 - 0,066/0,03 млн т
газ (извл.):
D1 - 0,00475 млрд м3
конденсат (геол./извл.):
D1 - 0,00022/0,00013 млн т', '{"oil": {"A": [7.0, "млн т"], "D1": [0.066, "млн т"]}, "gas": {"D1": [0.00475, "млрд м3"]}, "cond": {"D1": [0.00022, "млн т"]}}'),
(2026, '2026-01-02', 'Красноярский край', e'Быстрянская площадь', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'газ (извл.):
D2 - 0,330 млрд м3', '{"gas": {"D2": [0.33, "млрд м3"]}}'),
(2026, '2026-01-02', 'Оренбургская область', e'Соболевский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (геол./извл.):
C1 - 3,682/0,296 млн т,
D1(D3dm) - 51,27/1,54 млн т', '{"oil": {"C1": [3.682, "млн т"], "D1": [51.27, "млн т"]}}'),
(2026, '2026-01-02', 'Оренбургская область', e'Мраковский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (геол./извл.):
D1 - 157,546/45,135 млн т,
D1(D3dm) - 2550,421/76,512 млн т,
D2 - 1,35/0,616 млн т
газ (извл./геол.):
D1 - 6,021/6,021 млрд м3
конденсат (извл./геол.):
D1 - 0,68/0,423 млн т', '{"oil": {"D1": [2550.421, "млн т"], "D2": [1.35, "млн т"]}, "gas": {"D1": [6.021, "млрд м3"]}, "cond": {"D1": [0.68, "млн т"]}}'),
(2026, '2026-01-02', 'Оренбургская область', e'Западно-Мраковский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
D1 - 7,372 млн т,
D1(D3dm) - 28,9 млн т
газ (извл.):
D1 - 1,304 млрд м3
конденсат (извл.):
D1 - 0,028 млн т', '{"oil": {"D1": [28.9, "млн т"]}, "gas": {"D1": [1.304, "млрд м3"]}, "cond": {"D1": [0.028, "млн т"]}}'),
(2026, '2026-01-02', 'Оренбургская область', e'Дружный', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
D0 - 2,008 млн т,
D1 - 16,675 млн т,
D1 - 5,969 млн т
газ:
D1 - 1,845 млрд м3,
D1 - 1,704 млрд м3
конденсат:
D1 - 0,336 млн т', '{"oil": {"D0": [2.008, "млн т"], "D1": [5.969, "млн т"]}, "gas": {"D1": [1.704, "млрд м3"]}, "cond": {"D1": [0.336, "млн т"]}}'),
(2026, '2026-01-02', 'Оренбургская область', e'Владимировский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (геол./извл.):
D1 - 58,619/22,652 млн т
D1 - 2,029/0,605 млн т
D2 - 0,557/0,201 млн т
D1(D3dm) - 56,04/1,68 млн т
газ (геол./извл.):
D1 - 5,546/5,546 млрд м3
D1 - 1,554/1,554 млрд м3
D2 - 0,173/0,173 млрд м3
конденсат (геол./извл.):
D1 - 1,075/0,895 млн т', '{"oil": {"D1": [56.04, "млн т"], "D2": [0.557, "млн т"]}, "gas": {"D1": [1.554, "млрд м3"], "D2": [0.173, "млрд м3"]}, "cond": {"D1": [1.075, "млн т"]}}'),
(2026, '2026-01-02', 'Пермский край', e'Половодовский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (геол./извл.):
D1 - 0,978/0,315 млн т
D1(D3dm) - 83,259/2,498 млн т
газ (геол./извл.):
D1 - 0,039/0,039 млрд м3
конденсат (геол./извл.):
D1 - 0,003/0,001 млн т', '{"oil": {"D1": [83.259, "млн т"]}, "gas": {"D1": [0.039, "млрд м3"]}, "cond": {"D1": [0.003, "млн т"]}}'),
(2026, '2026-01-02', 'Пермский край', e'Вильвенский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (геол./извл.):
D1 - 1,778/0,572 млн т
D1(D3dm) - 153,379/4,601 млн т
газ (геол./извл.):
D1 - 0,073/0,073 млрд м3
конденсат (геол./извл.):
D1 - 0,005/0,002 млн т', '{"oil": {"D1": [153.379, "млн т"]}, "gas": {"D1": [0.073, "млрд м3"]}, "cond": {"D1": [0.005, "млн т"]}}'),
(2026, '2026-01-02', 'Пермский край', e'Черноречный', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (геол./извл.):
D1 - 2,145/0,691 млн т
D1(D3dm) - 184,337/5,530 млн т
газ (геол./извл.):
D1 - 0,088/0,088 млрд м3
конденсат (геол./извл.):
D1 - 0,006/0,002 млн т', '{"oil": {"D1": [184.337, "млн т"]}, "gas": {"D1": [0.088, "млрд м3"]}, "cond": {"D1": [0.006, "млн т"]}}'),
(2026, '2026-01-02', 'Пермский край', e'Ольховский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл./геол.):
D1 - 0,864/2,701 млн т
газ (извл./геол.):
D1 - 0,041/0,041 млрд м3', '{"oil": {"D1": [0.864, "млн т"]}, "gas": {"D1": [0.041, "млрд м3"]}}'),
(2026, '2026-01-02', 'Пермский край', e'Малыгинский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл./геол.):
D1 - 1,532/4,785 млн т
газ (извл./геол.):
D1 - 0,072/0,072 млрд м3
конденсат (извл./геол.):
D1 - 0,004/0,001 млн т', '{"oil": {"D1": [1.532, "млн т"]}, "gas": {"D1": [0.072, "млрд м3"]}, "cond": {"D1": [0.004, "млн т"]}}'),
(2026, '2026-01-02', 'Пермский край', e'Сестринский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (геол./извл.):
D1 - 0,974/0,314 млн т
D1(D3dm) - 74,921/2,248 млн т', '{"oil": {"D1": [74.921, "млн т"]}}'),
(2026, '2026-01-02', 'Пермский край', e'Новоильинский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
D1 - 3,247 млн т
газ (извл.):
D1 - 0,154 млрд м3', '{"oil": {"D1": [3.247, "млн т"]}, "gas": {"D1": [0.154, "млрд м3"]}}'),
(2026, '2026-01-02', 'Пермский край', e'Сурмогский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
D1 - 1,106 млн т
газ (извл.):
D1 - 0,053 млрд м3', '{"oil": {"D1": [1.106, "млн т"]}, "gas": {"D1": [0.053, "млрд м3"]}}'),
(2026, '2026-01-02', 'Пермский край', e'Центрально-Чекурский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
D1 - 0,882 млн т
газ (извл.):
D1 - 0,042 млрд м3', '{"oil": {"D1": [0.882, "млн т"]}, "gas": {"D1": [0.042, "млрд м3"]}}'),
(2026, '2026-01-02', 'Пермский край', e'Южно-Чекурский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
D1 - 1,247 млн т
газ (извл.):
D1 - 0,059 млрд м3', '{"oil": {"D1": [1.247, "млн т"]}, "gas": {"D1": [0.059, "млрд м3"]}}'),
(2026, '2026-01-02', 'Пермский край', e'Западно-Чекурский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
D1 - 0,934 млн т
газ (извл.):
D1 - 0,044 млрд м3', '{"oil": {"D1": [0.934, "млн т"]}, "gas": {"D1": [0.044, "млрд м3"]}}'),
(2026, '2026-01-02', 'Пермский край', e'Северо-Чекурский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
D1 - 1,563 млн т
газ (извл.):
D1 - 0,074 млрд м3', '{"oil": {"D1": [1.563, "млн т"]}, "gas": {"D1": [0.074, "млрд м3"]}}'),
(2026, '2026-01-02', 'Пермский край', e'Верхне-Вильвенский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (геол./извл.):
D1 - 2,417/0,778 млн т
D1(D3dm) - 201,438/6,043 млн т', '{"oil": {"D1": [201.438, "млн т"]}}'),
(2026, '2026-01-02', 'Пермский край', e'Нижне-Вильвенский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (геол./извл.):
D1 - 1,885/0,607 млн т
D1(D3dm) - 166,512/4,995 млн т', '{"oil": {"D1": [166.512, "млн т"]}}'),
(2026, '2026-01-02', 'Пермский край', e'Восточно-Вильвенский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (геол./извл.):
D1 - 2,774/0,894 млн т
D1(D3dm) - 238,704/7,161 млн т', '{"oil": {"D1": [238.704, "млн т"]}}'),
(2026, '2026-01-02', 'Пермский край', e'Северо-Вильвенский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (геол./извл.):
D1 - 1,396/0,450 млн т
газ (геол./извл.):
D1 - 0,066/0,066 млрд м3', '{"oil": {"D1": [1.396, "млн т"]}, "gas": {"D1": [0.066, "млрд м3"]}}'),
(2026, '2026-01-02', 'Республика Дагестан', e'Северо-Юбилейный', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.)
B1 - 0,102 млн т', '{"oil": {"B1": [0.102, "млн т"]}}'),
(2026, '2026-01-02', 'Республика Калмыкия', e'Моктинский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
D1 - 0,081 млн т,
D2 - 0,003 млн т;
газ (извл.)
C1 - 0,139 млрд м3,
C2 - 0,522 млрд м3,
D1 - 0,777 млрд м3,
D2 - 0,044 млрд м3;
конденсат (геол./извл.):
C1 - 0,002/0,001 млн т,
C2 - 0,008/0,006 млн т,
D1 - 0,036 млн т', '{"oil": {"D1": [0.081, "млн т"], "D2": [0.003, "млн т"]}, "gas": {"C1": [0.139, "млрд м3"], "C2": [0.522, "млрд м3"], "D1": [0.777, "млрд м3"], "D2": [0.044, "млрд м3"]}, "cond": {"C1": [0.002, "млн т"], "C2": [0.008, "млн т"], "D1": [0.036, "млн т"]}}'),
(2026, '2026-01-02', 'Республика Коми', e'Нямедский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'газ (извл.)
C1 - 0,007 млрд м3', '{"gas": {"C1": [0.007, "млрд м3"]}}'),
(2026, '2026-01-02', 'Республика Коми', e'Эжвадорский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'газ (извл.)
C1 - 0,414 млрд м3', '{"gas": {"C1": [0.414, "млрд м3"]}}'),
(2026, '2026-01-02', 'Республика Коми', e'Северо-Седъельский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'газ (извл.)
C1 - 0,039 млрд м3', '{"gas": {"C1": [0.039, "млрд м3"]}}'),
(2026, '2026-01-02', 'Республика Коми', e'Кушкоджский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'газ (извл.)
C1 - 0,090 млрд м3', '{"gas": {"C1": [0.09, "млрд м3"]}}'),
(2026, '2026-01-02', 'Республика Коми', e'Дорожный', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (геол./извл.):
C1 - 1,875/0,015 млн т
газ (извл.)
C1 - 0,487 млрд м3', '{"oil": {"C1": [1.875, "млн т"]}, "gas": {"C1": [0.487, "млрд м3"]}}'),
(2026, '2026-01-02', 'Ненецкий автономный округ', e'Шапкинский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (геол./извл.):
Dл - 23,920/10,764 млн т
D1 - 1,067/0,345 млн т
D2 - 0,973/0,197 млн т;
газ (геол./извл.):
D1 - 1,63/1,63 млрд м3
D2 - 0,133/0,133 млрд м3
C1 - 3,291/3,291 млрд м3', '{"oil": {"Dл": [23.92, "млн т"], "D1": [1.067, "млн т"], "D2": [0.973, "млн т"]}, "gas": {"D1": [1.63, "млрд м3"], "D2": [0.133, "млрд м3"], "C1": [3.291, "млрд м3"]}}'),
(2026, '2026-01-02', 'Республика Саха (Якутия)', e'Мунско-Сянский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
Dл - 785,35 млн т
газ (извл.):
Dл - 67,74 млрд м3', '{"oil": {"Dл": [785.35, "млн т"]}, "gas": {"Dл": [67.74, "млрд м3"]}}'),
(2026, '2026-01-02', 'Республика Саха (Якутия)', e'Ундюлюнгский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'газ (извл.):
Dл - 1178,979 млрд м3;
конденсат (извл.):
Dл - 31,12 млн т', '{"gas": {"Dл": [1178.979, "млрд м3"]}, "cond": {"Dл": [31.12, "млн т"]}}'),
(2026, '2026-01-02', 'Республика Саха (Якутия)', e'Жиганский 1', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
Dл - 459 млн т
газ (извл.):
Dл - 376,8 млрд м3', '{"oil": {"Dл": [459.0, "млн т"]}, "gas": {"Dл": [376.8, "млрд м3"]}}'),
(2026, '2026-01-02', 'Республика Саха (Якутия)', e'Жиганский 2', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
Dл - 512,4 млн т
газ (извл.):
Dл - 421,6 млрд м3', '{"oil": {"Dл": [512.4, "млн т"]}, "gas": {"Dл": [421.6, "млрд м3"]}}'),
(2026, '2026-01-02', 'Республика Саха (Якутия)', e'Жиганский 3', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
Dл - 384,1 млн т
газ (извл.):
Dл - 305,2 млрд м3', '{"oil": {"Dл": [384.1, "млн т"]}, "gas": {"Dл": [305.2, "млрд м3"]}}'),
(2026, '2026-01-02', 'Томская область', e'Северо-Пудинский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (геол./извл.):
D1 - 4,382/1,406 млн т
D2 - 1,228/0,393 млн т
газ (геол./извл.):
D1 - 0,842/0,842 млрд м3', '{"oil": {"D1": [4.382, "млн т"], "D2": [1.228, "млн т"]}, "gas": {"D1": [0.842, "млрд м3"]}}'),
(2026, '2026-01-02', 'Томская область', e'Пудинский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (геол./извл.):
D1 - 3,147/1,011 млн т
газ (геол./извл.):
D1 - 0,604/0,604 млрд м3', '{"oil": {"D1": [3.147, "млн т"]}, "gas": {"D1": [0.604, "млрд м3"]}}'),
(2026, '2026-01-02', 'Томская область', e'Южно-Пудинский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (геол./извл.):
D1 - 2,904/0,933 млн т
газ (геол./извл.):
D1 - 0,557/0,557 млрд м3', '{"oil": {"D1": [2.904, "млн т"]}, "gas": {"D1": [0.557, "млрд м3"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ', e'Северо-Ляминский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
C1 - 1,274 млн т
газ (извл.):
C1 - 0,118 млрд м3', '{"oil": {"C1": [1.274, "млн т"]}, "gas": {"C1": [0.118, "млрд м3"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ', e'Южно-Ляминский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
C1 - 0,944 млн т
газ (извл.):
C1 - 0,086 млрд м3', '{"oil": {"C1": [0.944, "млн т"]}, "gas": {"C1": [0.086, "млрд м3"]}}'),
(2026, '2026-01-02', 'Ямало-Ненецкий автономный округ', e'Западно-Тамбейский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'газ (извл.):
C1 - 128,6 млрд м3
конденсат (извл.):
C1 - 7,42 млн т', '{"gas": {"C1": [128.6, "млрд м3"]}, "cond": {"C1": [7.42, "млн т"]}}'),
(2026, '2026-01-02', 'Тюменская область', e'Карабашский 5-1', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (геол./извл.):
Dл - 415,154/101,784 млн т,
D1 - 80,741/18,545 млн т,
D2 - 21,24/5,435 млн т;
газ (геол./извл.):
Dл - 50,734/50,734 млрд м3,
D1 - 3,356/3,356 млрд м3,
D2 - 7,025/7,025 млрд м3', '{"oil": {"Dл": [415.154, "млн т"], "D1": [80.741, "млн т"], "D2": [21.24, "млн т"]}, "gas": {"Dл": [50.734, "млрд м3"], "D1": [3.356, "млрд м3"], "D2": [7.025, "млрд м3"]}}'),
(2026, '2026-01-02', 'Тюменская область', e'Карабашский 7-2', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (геол./извл.):
Dл - 227,723/53,993 млн т,
D1 - 56,421/13,606 млн т,
D2 - 52,753/13,646 млн т;
газ (геол./извл.):
D1 - 1,373/1,373 млрд м3,
D2 - 2,651/2,651 млрд м3', '{"oil": {"Dл": [227.723, "млн т"], "D1": [56.421, "млн т"], "D2": [52.753, "млн т"]}, "gas": {"D1": [1.373, "млрд м3"], "D2": [2.651, "млрд м3"]}}'),
(2026, '2026-01-02', 'Ставропольский край', e'Южно-Серафимовский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (геол./извл.):
D1 - 0,058/0,011 млн т;
газ (геол./извл.):
D0 - 1,4 млрд м3
D1 - 0,009/0,009 млрд м3,
C1 - 0,425 млрд м3,
C2 - 0,149 млрд м3,
конденсат (извл.)
C1 - 0,027 млн т,
C2 - 0,011 млн т', '{"oil": {"D1": [0.058, "млн т"]}, "gas": {"D0": [1.4, "млрд м3"], "D1": [0.009, "млрд м3"], "C1": [0.425, "млрд м3"], "C2": [0.149, "млрд м3"]}, "cond": {"C1": [0.027, "млн т"], "C2": [0.011, "млн т"]}}'),
(2026, '2026-01-02', 'Ставропольский край', e'Кучерлинский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'газ (извл.)
C1 - 1,0 млрд м3', '{"gas": {"C1": [1.0, "млрд м3"]}}'),
(2026, '2026-01-02', 'Ставропольский край', e'Веселовский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'газ (извл.)
C1 - 0,563 млрд м3', '{"gas": {"C1": [0.563, "млрд м3"]}}'),
(2026, '2026-01-02', 'Ставропольский край, Кабардино-Балкарская Республика', e'Моздокский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (геол./извл.):
Dл - 4,261/1,811 млн т
D1 - 11,981/5,632 млн т;
D1(Pg3chd+bt2) - 456,1/22,805
C1 - 1,158/0,579 млн т', '{"oil": {"Dл": [4.261, "млн т"], "D1": [11.981, "млн т"], "C1": [1.158, "млн т"]}}'),
(2026, '2026-01-02', 'Удмуртская Республика', e'Чекеровский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (геол./извл.)
B1 - 0,324/0,058 млн т
D1 - 3,15 млн т', '{"oil": {"B1": [0.324, "млн т"], "D1": [3.15, "млн т"]}}'),
(2026, '2026-01-02', 'Удмуртская Республика', e'Никольский участок', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
D0 - 2,358 млн т', '{"oil": {"D0": [2.358, "млн т"]}}'),
(2026, '2026-01-02', 'Удмуртская Республика', e'Карсовайский участок', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
D0 - 1,642 млн т', '{"oil": {"D0": [1.642, "млн т"]}}'),
(2026, '2026-01-02', 'Удмуртская Республика', e'Южно-Киенгопский участок', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
D0 - 1,944 млн т', '{"oil": {"D0": [1.944, "млн т"]}}'),
(2026, '2026-01-02', 'Ульяновская область', e'Южно-Репьевский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (геол./извл.):
D1 - 0,478/0,143 млн т', '{"oil": {"D1": [0.478, "млн т"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ', e'Имилорский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
C1 - 9,154 млн т
газ (извл.):
C1 - 1,173 млрд м3', '{"oil": {"C1": [9.154, "млн т"]}, "gas": {"C1": [1.173, "млрд м3"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ', e'Западно-Имилорский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
C1 - 3,784 млн т
газ (извл.):
C1 - 0,486 млрд м3', '{"oil": {"C1": [3.784, "млн т"]}, "gas": {"C1": [0.486, "млрд м3"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ', e'Северо-Имилорский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
C1 - 4,265 млн т
газ (извл.):
C1 - 0,548 млрд м3', '{"oil": {"C1": [4.265, "млн т"]}, "gas": {"C1": [0.548, "млрд м3"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ', e'Западно-Салымский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (геол./извл.):
D1 - 15,417/4,625 млн т
газ (геол./извл.):
D1 - 1,024/1,024 млрд м3', '{"oil": {"D1": [15.417, "млн т"]}, "gas": {"D1": [1.024, "млрд м3"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ', e'Южно-Салымский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (геол./извл.):
D1 - 11,328/3,398 млн т
газ (геол./извл.):
D1 - 0,752/0,752 млрд м3', '{"oil": {"D1": [11.328, "млн т"]}, "gas": {"D1": [0.752, "млрд м3"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ', e'Восточно-Салымский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (геол./извл.):
D1 - 13,806/4,142 млн т
газ (геол./извл.):
D1 - 0,916/0,916 млрд м3', '{"oil": {"D1": [13.806, "млн т"]}, "gas": {"D1": [0.916, "млрд м3"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ', e'Северо-Салымский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (геол./извл.):
D1 - 10,274/3,082 млн т
газ (геол./извл.):
D1 - 0,681/0,681 млрд м3', '{"oil": {"D1": [10.274, "млн т"]}, "gas": {"D1": [0.681, "млрд м3"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ', e'Западно-Угутский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
C1 - 5,712 млн т
газ (извл.):
C1 - 0,734 млрд м3', '{"oil": {"C1": [5.712, "млн т"]}, "gas": {"C1": [0.734, "млрд м3"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ', e'Восточно-Угутский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
C1 - 6,894 млн т
газ (извл.):
C1 - 0,885 млрд м3', '{"oil": {"C1": [6.894, "млн т"]}, "gas": {"C1": [0.885, "млрд м3"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ', e'Южно-Угутский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
C1 - 4,336 млн т
газ (извл.):
C1 - 0,557 млрд м3', '{"oil": {"C1": [4.336, "млн т"]}, "gas": {"C1": [0.557, "млрд м3"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ', e'Северо-Угутский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
C1 - 7,148 млн т
газ (извл.):
C1 - 0,919 млрд м3', '{"oil": {"C1": [7.148, "млн т"]}, "gas": {"C1": [0.919, "млрд м3"]}}'),
(2026, '2026-01-02', 'Чукотский автономный округ', e'Верхне-Телекайский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'золото рудное:
C1 - 7,2 т', '{}'),
(2026, '2026-01-02', 'Чукотский автономный округ', e'Нижне-Телекайский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'золото рудное:
C1 - 5,8 т', '{}'),
(2026, '2026-01-02', 'Чукотский автономный округ', e'Южно-Телекайский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'золото рудное:
C1 - 6,4 т', '{}'),
(2026, '2026-01-02', 'Ямало-Ненецкий автономный округ', e'Восточно-Тамбейский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'газ (извл.):
C1 - 145,8 млрд м3
конденсат (извл.):
C1 - 8,91 млн т', '{"gas": {"C1": [145.8, "млрд м3"]}, "cond": {"C1": [8.91, "млн т"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Карымский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.)
Д1 - 1,2 млн.т.
Д2 - 0,3 млн.т.
газ
Д1 - 0,2 млрд.м3
Д2 - 0,4 млрд.м3', '{"oil": {}, "gas": {}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Кулингурский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл)
Д1 - 11,0 млн.т.
Д2 - 0,8 млн.т.
газ
Д1 - 1,3 млрд.м3
Д2 - 1,0 млрд.м3', '{"oil": {}, "gas": {}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Ляминский 20', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл)
Д1 - 14,0 млн.т
Д2 - 2,7 млн. т', '{"oil": {}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Малобыстринский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл)
Д1 - 11,5 млн.т
Д2 - 0,5 млн.т
газ
Д1 - 4,2 млрд.м3
Д2 - 1,3 млрд.м3', '{"oil": {}, "gas": {}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Малорогожниковский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.)
Дп - 2,1 млн.т
Д1 - 2,1 млн.т
Д2 - 0,1 млн.т', '{"oil": {}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Моимский 1', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
Д1 - 13,5 млн.т.
Д2 - 0,5 млн.т.
газ (извл.):
Д1 - 0,4 млрд. м3.
Д2 - 0,9 млрд. м3.', '{"oil": {}, "gas": {}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Моимский 2', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
Д1 - 11,5 млн.т.
Д2 - 0,5 млн.т.
газ (извл.):
Д1 - 0,3 млрд. м3.
Д2 - 0,8 млрд. м3.', '{"oil": {}, "gas": {}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Панлорский 2', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл)
Д1 - 8,2 млн.т
Д2 - 0,3 млн.т
газ
Д2 - 2,0 млрд.м3', '{"oil": {}, "gas": {}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Похорский 2', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл)
Д1 - 5,0 млн.т.
Д2 - 0,8 млн.т.
газ
Д1 - 0,7 млрд. м3
Д2 - 1,4 млрд м3', '{"oil": {}, "gas": {}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Русотовый', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.)
D0 - 4,125 млн т
Dп - 4,102 млн т
D1 - 1,5 млн т', '{"oil": {"D0": [4.125, "млн т"], "D1": [1.5, "млн т"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Северо-Ай-Тимский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.)
D1 - 18,0 млн т
D2 - 0,4 млн т', '{"oil": {"D1": [18.0, "млн т"], "D2": [0.4, "млн т"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Северо-Заозерный 1', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.)
Dл - 0,031 млн т
D1 - 4,4 млн т
D2 - 0,5 млн т
газ
D2 - 0,3 млрд м3', '{"oil": {"Dл": [0.031, "млн т"], "D1": [4.4, "млн т"], "D2": [0.5, "млн т"]}, "gas": {"D2": [0.3, "млрд м3"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Северо-Заозерный 2', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.)
D1 - 6,4 млн т
D2 - 0,6 млн т
газ
D2 - 0,4 млрд м3', '{"oil": {"D1": [6.4, "млн т"], "D2": [0.6, "млн т"]}, "gas": {"D2": [0.4, "млрд м3"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Северо-Яхлинский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.)
D1 - 2,2 млн т
D2 - 0,2 млн т', '{"oil": {"D1": [2.2, "млн т"], "D2": [0.2, "млн т"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Северо-Тунгольский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.)
Dл - 2,481 млн т
D1 - 10,6 млн т
D2 - 1,2 млн т
газ
D1 - 0,3 млрд м3
D2 - 0,3 млрд м3', '{"oil": {"Dл": [2.481, "млн т"], "D1": [10.6, "млн т"], "D2": [1.2, "млн т"]}, "gas": {"D1": [0.3, "млрд м3"], "D2": [0.3, "млрд м3"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Сергинский 17', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.)
D1 - 15,5 млн т
D2 - 0,5 млн т
газ (извл.)
D1 - 0,5 млрд м3
D2 - 1,1 млрд м3', '{"oil": {"D1": [15.5, "млн т"], "D2": [0.5, "млн т"]}, "gas": {"D1": [0.5, "млрд м3"], "D2": [1.1, "млрд м3"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Сергинский 18', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.)
D1 - 17,5 млн т
D2 - 0,6 млн т
газ (извл.)
D1 - 0,5 млрд м3
D2 - 1,2 млрд м3', '{"oil": {"D1": [17.5, "млн т"], "D2": [0.6, "млн т"]}, "gas": {"D1": [0.5, "млрд м3"], "D2": [1.2, "млрд м3"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Терпеевский 1', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.)
D1 - 8,6 млн т
D2 - 0,4 млн т
газ
D1 - 0,3 млрд м3
D2 - 0,6 млрд м3', '{"oil": {"D1": [8.6, "млн т"], "D2": [0.4, "млн т"]}, "gas": {"D1": [0.3, "млрд м3"], "D2": [0.6, "млрд м3"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Терпеевский 2', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.)
D1 - 7,4 млн т
D2 - 0,3 млн т
газ
D1 - 0,2 млрд м3
D2 - 0,5 млрд м3', '{"oil": {"D1": [7.4, "млн т"], "D2": [0.3, "млн т"]}, "gas": {"D1": [0.2, "млрд м3"], "D2": [0.5, "млрд м3"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Терпеевский 3', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.)
D1 - 6,9 млн т
D2 - 0,3 млн т
газ
D1 - 0,2 млрд м3
D2 - 0,4 млрд м3', '{"oil": {"D1": [6.9, "млн т"], "D2": [0.3, "млн т"]}, "gas": {"D1": [0.2, "млрд м3"], "D2": [0.4, "млрд м3"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Торъеганский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.)
D1 - 12,7 млн т
D2 - 0,5 млн т
газ
D1 - 0,4 млрд м3
D2 - 0,9 млрд м3', '{"oil": {"D1": [12.7, "млн т"], "D2": [0.5, "млн т"]}, "gas": {"D1": [0.4, "млрд м3"], "D2": [0.9, "млрд м3"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Тундринский 4', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.)
D1 - 4,8 млн т
D2 - 0,2 млн т', '{"oil": {"D1": [4.8, "млн т"], "D2": [0.2, "млн т"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Тундринский 5', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.)
D1 - 5,1 млн т
D2 - 0,2 млн т', '{"oil": {"D1": [5.1, "млн т"], "D2": [0.2, "млн т"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Тундринский 6', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.)
D1 - 5,6 млн т
D2 - 0,3 млн т', '{"oil": {"D1": [5.6, "млн т"], "D2": [0.3, "млн т"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Усть-Тегусский 7', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.)
D1 - 14,8 млн т
D2 - 0,6 млн т
газ
D1 - 0,5 млрд м3
D2 - 1,0 млрд м3', '{"oil": {"D1": [14.8, "млн т"], "D2": [0.6, "млн т"]}, "gas": {"D1": [0.5, "млрд м3"], "D2": [1.0, "млрд м3"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Усть-Тегусский 8', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.)
D1 - 16,1 млн т
D2 - 0,7 млн т
газ
D1 - 0,6 млрд м3
D2 - 1,1 млрд м3', '{"oil": {"D1": [16.1, "млн т"], "D2": [0.7, "млн т"]}, "gas": {"D1": [0.6, "млрд м3"], "D2": [1.1, "млрд м3"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Шеркалинский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.)
D1 - 3,7 млн т
D2 - 0,1 млн т', '{"oil": {"D1": [3.7, "млн т"], "D2": [0.1, "млн т"]}}'),
(2026, '2026-01-02', 'Ямало-Ненецкий автономный округ', e'Южно-Тамбейский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'газ (извл.)
C1 - 132,4 млрд м3
конденсат (извл.)
C1 - 7,8 млн т', '{"gas": {"C1": [132.4, "млрд м3"]}, "cond": {"C1": [7.8, "млн т"]}}'),
(2026, '2026-01-02', 'Ямало-Ненецкий автономный округ', e'Северо-Тамбейский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'газ (извл.)
C1 - 118,9 млрд м3
конденсат (извл.)
C1 - 6,9 млн т', '{"gas": {"C1": [118.9, "млрд м3"]}, "cond": {"C1": [6.9, "млн т"]}}'),
(2026, '2026-01-02', 'Ямало-Ненецкий автономный округ', e'Западно-Сеяхинский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'газ (извл.)
C1 - 94,5 млрд м3
конденсат (извл.)
C1 - 5,1 млн т', '{"gas": {"C1": [94.5, "млрд м3"]}, "cond": {"C1": [5.1, "млн т"]}}'),
(2026, '2026-01-02', 'Ямало-Ненецкий автономный округ', e'Восточно-Сеяхинский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'газ (извл.)
C1 - 88,7 млрд м3
конденсат (извл.)
C1 - 4,8 млн т', '{"gas": {"C1": [88.7, "млрд м3"]}, "cond": {"C1": [4.8, "млн т"]}}'),
(2026, '2026-01-02', 'Ямало-Ненецкий автономный округ', e'Салмановский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.)
C1 - 24,6 млн т
газ (извл.)
C1 - 62,3 млрд м3
конденсат (извл.)
C1 - 3,4 млн т', '{"oil": {"C1": [24.6, "млн т"]}, "gas": {"C1": [62.3, "млрд м3"]}, "cond": {"C1": [3.4, "млн т"]}}'),
(2026, '2026-01-02', 'Ямало-Ненецкий автономный округ', e'Утренний', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'газ (извл.)
C1 - 198,4 млрд м3
конденсат (извл.)
C1 - 12,6 млн т', '{"gas": {"C1": [198.4, "млрд м3"]}, "cond": {"C1": [12.6, "млн т"]}}'),
(2026, '2026-01-02', 'Ямало-Ненецкий автономный округ', e'Геофизический', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'газ (извл.)
C1 - 76,8 млрд м3', '{"gas": {"C1": [76.8, "млрд м3"]}}'),
(2026, '2026-01-02', 'Ямало-Ненецкий автономный округ', e'Арктический', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'газ (извл.)
C1 - 154,1 млрд м3
конденсат (извл.)
C1 - 8,3 млн т', '{"gas": {"C1": [154.1, "млрд м3"]}, "cond": {"C1": [8.3, "млн т"]}}'),
(2026, '2026-01-02', 'Ямало-Ненецкий автономный округ', e'Нурминский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.)
C1 - 9,2 млн т
газ (извл.)
C1 - 14,7 млрд м3', '{"oil": {"C1": [9.2, "млн т"]}, "gas": {"C1": [14.7, "млрд м3"]}}'),
(2026, '2026-01-02', 'Ямало-Ненецкий автономный округ', e'Харасавэйский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'газ (извл.)
C1 - 247,3 млрд м3
конденсат (извл.)
C1 - 15,4 млн т', '{"gas": {"C1": [247.3, "млрд м3"]}, "cond": {"C1": [15.4, "млн т"]}}'),
(2026, '2026-01-02', 'Ямало-Ненецкий автономный округ', e'Крузенштернский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'газ (извл.)
C1 - 186,2 млрд м3
конденсат (извл.)
C1 - 11,8 млн т', '{"gas": {"C1": [186.2, "млрд м3"]}, "cond": {"C1": [11.8, "млн т"]}}'),
(2026, '2026-01-02', 'Ямало-Ненецкий автономный округ', e'Ленинградский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'газ (извл.)
C1 - 321,5 млрд м3
конденсат (извл.)
C1 - 18,9 млн т', '{"gas": {"C1": [321.5, "млрд м3"]}, "cond": {"C1": [18.9, "млн т"]}}'),
(2026, '2026-01-02', 'Ямало-Ненецкий автономный округ', e'Русановский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'газ (извл.)
C1 - 402,7 млрд м3
конденсат (извл.)
C1 - 24,1 млн т', '{"gas": {"C1": [402.7, "млрд м3"]}, "cond": {"C1": [24.1, "млн т"]}}'),
(2026, '2026-01-02', 'Ямало-Ненецкий автономный округ', e'Каменномысский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'газ (извл.)
C1 - 156,9 млрд м3
конденсат (извл.)
C1 - 9,6 млн т', '{"gas": {"C1": [156.9, "млрд м3"]}, "cond": {"C1": [9.6, "млн т"]}}');