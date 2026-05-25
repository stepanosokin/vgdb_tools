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
D1(D3dm) - 51,27/1,54 млн т', '{"oil": {"C1": [3.682, "млн т"]}}'),
(2026, '2026-01-02', 'Оренбургская область', e'Мраковский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (геол./извл.):
D1 - 157,546/45,135 млн т,
D1(D3dm) - 2550,421/76,512 млн т,
D2 - 1,35/0,616 млн т
газ (извл./геол.):
D1 - 6,021/6,021 млрд м3
конденсат (извл./геол.):
D1 - 0,68/0,423 млн т', '{"oil": {"D1": [157.546, "млн т"], "D2": [1.35, "млн т"]}, "gas": {"D1": [6.021, "млрд м3"]}, "cond": {"D1": [0.68, "млн т"]}}'),
(2026, '2026-01-02', 'Оренбургская область', e'Западно-Мраковский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
D1 - 7,372 млн т,
D1(D3dm) - 28,9 млн т
газ (извл.):
D1 - 1,304 млрд м3
конденсат (извл.):
D1 - 0,028 млн т', '{"oil": {"D1": [7.372, "млн т"]}, "gas": {"D1": [1.304, "млрд м3"]}, "cond": {"D1": [0.028, "млн т"]}}'),
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
D1 - 1,075/0,895 млн т', '{"oil": {"D1": [2.029, "млн т"], "D2": [0.557, "млн т"]}, "gas": {"D1": [1.554, "млрд м3"], "D2": [0.173, "млрд м3"]}, "cond": {"D1": [1.075, "млн т"]}}'),
(2026, '2026-01-02', 'Пермский край', e'Половодовский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (геол./извл.):
D1 - 0,978/0,315 млн т
D1(D3dm) - 83,259/2,498 млн т
газ (геол./извл.):
D1 - 0,039/0,039 млрд м3
конденсат (геол./извл.):
D1 - 0,003/0,001 млн т', '{"oil": {"D1": [0.978, "млн т"]}, "gas": {"D1": [0.039, "млрд м3"]}, "cond": {"D1": [0.003, "млн т"]}}'),
(2026, '2026-01-02', 'Пермский край', e'Вильвенский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (геол./извл.):
D1 - 1,778/0,572 млн т
D1(D3dm) - 153,379/4,601 млн т
газ (геол./извл.):
D1 - 0,073/0,073 млрд м3
конденсат (геол./извл.):
D1 - 0,005/0,002 млн т', '{"oil": {"D1": [1.778, "млн т"]}, "gas": {"D1": [0.073, "млрд м3"]}, "cond": {"D1": [0.005, "млн т"]}}'),
(2026, '2026-01-02', 'Пермский край', e'Черноречный', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (геол./извл.):
Dл - 0,109/0,043 млн.т.
D1 - 2,241/0,713 млн.т
D1(D3dm) - 172,442/5,173 млн.т
газ (геол./извл.):
D1 - 0,094/0,094 млрд.м3.
конденсат (геол./извл.):
D1 - 0,006/0,003 млн.т', '{"oil": {"Dл": [0.109, "млнт"], "D1": [2.241, "млнт"]}, "gas": {"D1": [0.094, "млрдм3"]}, "cond": {"D1": [0.006, "млнт"]}}'),
(2026, '2026-01-02', 'Пермский край', e'Ольховский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл./геол.):
Dл - 0,389/0,119 млн.т
D1 - 3,334/1,315 млн.т
D1(D3dm) - 386,455/11,594 млн.т
газ (извл./геол.):
D1 - 0,097/0,097 млрд.м3;
конденсат (извл./геол.):
D1 - 0,01/0,003 млн.т', '{"oil": {"Dл": [0.389, "млнт"], "D1": [3.334, "млнт"]}, "gas": {"D1": [0.097, "млрдм3"]}, "cond": {"D1": [0.01, "млнт"]}}'),
(2026, '2026-01-02', 'Пермский край', e'Малыгинский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл./геол.):
D1 - 1,78/0,614 млн.т
D1(D3dm) - 151,245/4,537 млн.т.
газ (извл./геол.):
D1 - 0,076/0,076 млрд.м3;
конденсат (извл./геол.):
D1 - 0,005/0,002 млн.т', '{"oil": {"D1": [1.78, "млнт"]}, "gas": {"D1": [0.076, "млрдм3"]}, "cond": {"D1": [0.005, "млнт"]}}'),
(2026, '2026-01-02', 'Пермский край', e'Сестринский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (геол./извл.):
D1 - 2,439/0,748 млн.т,
D1(D3dm)-245,679/7,340 млн.т
газ (геол./извл.):
D1 - 0,015/0,015 млрд. м3', '{"oil": {"D1": [2.439, "млнт"]}, "gas": {"D1": [0.015, "млрд м3"]}}'),
(2026, '2026-01-02', 'Пермский край', e'Бердышевский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
D1 - 0,266 млн.т,
D1(D3dm)-2,176 млн.т', '{"oil": {"D1": [0.266, "млнт"]}}'),
(2026, '2026-01-02', 'Пермский край', e'Новоильинский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
D1 - 1,4 млн.т', '{"oil": {"D1": [1.4, "млнт"]}}'),
(2026, '2026-01-02', 'Пермский край', e'Сурмогский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
D1 - 0,4 млн.т,
D2 - 0,2 млн.т', '{"oil": {"D1": [0.4, "млнт"], "D2": [0.2, "млнт"]}}'),
(2026, '2026-01-02', 'Пермский край', e'Яйвинский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
D1 - 0,3 млн.т,
D2 - 0,2 млн.т', '{"oil": {"D1": [0.3, "млнт"], "D2": [0.2, "млнт"]}}'),
(2026, '2026-01-02', 'Пермский край', e'Центрально-Чекурский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
D1 - 0,4 млн.т,
D2 - 0,3 млн.т', '{"oil": {"D1": [0.4, "млнт"], "D2": [0.3, "млнт"]}}'),
(2026, '2026-01-02', 'Пермский край', e'Мельничный', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
D1 - 0,3 млн.т,
D2 - 0,2 млн.т', '{"oil": {"D1": [0.3, "млнт"], "D2": [0.2, "млнт"]}}'),
(2026, '2026-01-02', 'Пермский край', e'Восточно-Яринский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
D1 - 0,2 млн.т,
D2 - 0,1 млн.т', '{"oil": {"D1": [0.2, "млнт"], "D2": [0.1, "млнт"]}}'),
(2026, '2026-01-02', 'Пермский край', e'Конюшорский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
D0 - 0,25 млн.т', '{"oil": {"D0": [0.25, "млнт"]}}'),
(2026, '2026-01-02', 'Пермский край', e'Летовский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
D0 - 0,138 млн.т', '{"oil": {"D0": [0.138, "млнт"]}}'),
(2026, '2026-01-02', 'Пермский край', e'Мыльниковский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть:
D1 - 2,2 млн.т', '{"oil": {"D1": [2.2, "млнт"]}}'),
(2026, '2026-01-02', 'Пермский край', e'Колыновский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
D1 - 0,3 млн.т,
D2 - 0,2 млн.т', '{"oil": {"D1": [0.3, "млнт"], "D2": [0.2, "млнт"]}}'),
(2026, '2026-01-02', 'Республика Башкортостан', e'Южно-Салаевский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (геол./извл.):
Dл - 2,201/0,927 млн.т
D1(D3dm) - 34,453/1,034 млн.т', '{"oil": {"Dл": [2.201, "млнт"]}}'),
(2026, '2026-01-02', 'Республика Башкортостан', e'Куязинский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл./геол.):
D0 - 1,752/0,979 млн.т,
Dл - 2,2/1,229 млн.т,
D1 - 1,649/0,644 млн.т,
D1(D3dm) - 551,115/16,533 млн.т;
газ (извл./геол.):
D2 - 0,113/0,113 млрд.м3', '{"oil": {"D0": [1.752, "млнт"], "Dл": [2.2, "млнт"], "D1": [1.649, "млнт"]}, "gas": {"D2": [0.113, "млрдм3"]}}'),
(2026, '2026-01-02', 'Республика Башкортостан', e'Муслюмовский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'газ
C1 - 1,584 млрд.м3', '{"gas": {"C1": [1.584, "млрдм3"]}}'),
(2026, '2026-01-02', 'Республика Башкортостан', e'Кармаскалинский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
D1 - 0,4 млн.т,
D2 - 0,3 млн.т', '{"oil": {"D1": [0.4, "млнт"], "D2": [0.3, "млнт"]}}'),
(2026, '2026-01-02', 'Республика Башкортостан', e'Кармышевский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
D1 - 0,3 млн.т,
D2 - 0,2 млн.т', '{"oil": {"D1": [0.3, "млнт"], "D2": [0.2, "млнт"]}}'),
(2026, '2026-01-02', 'Республика Башкортостан', e'Тюр-Седякский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
D0 - 1,671 млн.т', '{"oil": {"D0": [1.671, "млнт"]}}'),
(2026, '2026-01-02', 'Республика Башкортостан', e'Рассветовский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
D1 - 0,4 млн.т,
D2 - 0,2 млн.т', '{"oil": {"D1": [0.4, "млнт"], "D2": [0.2, "млнт"]}}'),
(2026, '2026-01-02', 'Республика Дагестан', e'Наказухский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.)
B1 - 0.003 млн.т', '{"oil": {"B1": [0.003, "млнт"]}}'),
(2026, '2026-01-02', 'Республика Дагестан', e'Октябрьский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.)
B1 - 0.003 млн.т', '{"oil": {"B1": [0.003, "млнт"]}}'),
(2026, '2026-01-02', 'Республика Дагестан', e'Манасозень', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'газ (извл.)
D1 - 0,5 млрд.м3', '{"gas": {"D1": [0.5, "млрдм3"]}}'),
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
Dл - 138 млн.т.
газ (извл.):
Dл - 299 млрд.м3', '{"oil": {"Dл": [138.0, "млнт"]}, "gas": {"Dл": [299.0, "млрдм3"]}}'),
(2026, '2026-01-02', 'Республика Саха (Якутия)', e'Восточно-Линденский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'газ (извл.):
Dл - 781,5 млрд.м3;
конденсат (извл.):
Dл - 16,9 млн.т.', '{"gas": {"Dл": [781.5, "млрдм3"]}, "cond": {"Dл": [16.9, "млнт"]}}'),
(2026, '2026-01-02', 'Республика Саха (Якутия)', e'Соболох-Маянский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'газ (извл.):
Dл - 798,7 млрд.м3;
конденсат (извл.):
Dл - 11,5 млн.т.', '{"gas": {"Dл": [798.7, "млрдм3"]}, "cond": {"Dл": [11.5, "млнт"]}}'),
(2026, '2026-01-02', 'Республика Саха (Якутия)', e'Усть-Вилюйский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'газ (извл.):
C1 - 0,762 млрд.м3', '{"gas": {"C1": [0.762, "млрдм3"]}}'),
(2026, '2026-01-02', 'Самарская область', e'Северо-Иргизский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
D1 - 5,134 млн.т;
D1(D3dm) - 8,9 млн.т.
газ (извл.):
D1 - 0,604 млрд. м3;
конденсат (извл.):
D1 - 0,01 млн.т', '{"oil": {"D1": [5.134, "млнт"]}, "gas": {"D1": [0.604, "млрд м3"]}, "cond": {"D1": [0.01, "млнт"]}}'),
(2026, '2026-01-02', 'Самарская область', e'Восточно-Каревский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть
Dл - 11,058 млн.т
D1(D3dm) - 12,96 млн.т', '{"oil": {"Dл": [11.058, "млнт"]}}'),
(2026, '2026-01-02', 'Самарская область', e'Зеленогорский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (геол./извл.)
D0 - 4,901/1,934 млн.т.
Dл - 4,876/1,881 млн.т.
D1(D3dm) - 315,904/9,477 млн.т.', '{"oil": {"D0": [4.901, "млнт"], "Dл": [4.876, "млнт"]}}'),
(2026, '2026-01-02', 'Саратовская область', e'Гвардейский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'газ (геол./извл.):
C1 - 0,5/0,5 млрд. м3
C2 - 0,8/0,8 млрд. м3', '{"gas": {"C1": [0.5, "млрд м3"], "C2": [0.8, "млрд м3"]}}'),
(2026, '2026-01-02', 'Саратовская область', e'Широко-Карамышский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
B1 - 0,092 млн.т;
газ (извл.):
A - 0,468 млрд. м3;
B1 - 0,612 млрд. м3', '{"oil": {"B1": [0.092, "млнт"]}, "gas": {"A": [0.468, "млрд м3"], "B1": [0.612, "млрд м3"]}}'),
(2026, '2026-01-02', 'Саратовская область', e'Смирновский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'газ (извл.):
C1 - 0,306 млрд. м3;
C2 - 0,682 млрд. м3', '{"gas": {"C1": [0.306, "млрд м3"], "C2": [0.682, "млрд м3"]}}'),
(2026, '2026-01-02', 'Саратовская область', e'Аряшский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
C1 - 0,025 млн.т;
газ (извл.):
C1 - 0,028 млрд. м3', '{"oil": {"C1": [0.025, "млнт"]}, "gas": {"C1": [0.028, "млрд м3"]}}'),
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
(2026, '2026-01-02', 'Ульяновская область', e'Старокулаткинский участок', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (геол./извл.):
C1 - 0,531/0,133 млн.т
D0 - 0,964/0,241 млн.т', '{"oil": {"C1": [0.531, "млнт"], "D0": [0.964, "млнт"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Тарховский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (геол./извл.):
A - 0,856/0,056 млн.т
B1 - 1,056/0,257 млн.т
D0 - 0,380/0,091 млн.т
D1 - 2,704/0,891 млн.т
D2 - 13,471/2,021 млн.т', '{"oil": {"A": [0.856, "млнт"], "B1": [1.056, "млнт"], "D0": [0.38, "млнт"], "D1": [2.704, "млнт"], "D2": [13.471, "млнт"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Турьяхский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
D0 - 5,802 млн.т
Dл - 2,032 млн.т
D1 - 3,283 млн.т
D2 - 3,973 млн.т', '{"oil": {"D0": [5.802, "млнт"], "Dл": [2.032, "млнт"], "D1": [3.283, "млнт"], "D2": [3.973, "млнт"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Южно-Чистинный 1', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
D0 - 5,835 млн.т
Dл - 1,291 млн.т
D1 - 7,461 млн.т
D2 - 2,947 млн.т', '{"oil": {"D0": [5.835, "млнт"], "Dл": [1.291, "млнт"], "D1": [7.461, "млнт"], "D2": [2.947, "млнт"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Южно-Чистинный 2', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
D0 - 13,933 млн.т
Dл - 6,826 млн.т
D1 - 6,597 млн.т
D2 - 1,622 млн.т', '{"oil": {"D0": [13.933, "млнт"], "Dл": [6.826, "млнт"], "D1": [6.597, "млнт"], "D2": [1.622, "млнт"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Южно-Чистинный 3', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
D0 - 6,482 млн.т
Dл - 1,438 млн.т
D1 - 4,266 млн.т
D2 - 1,774 млн.т', '{"oil": {"D0": [6.482, "млнт"], "Dл": [1.438, "млнт"], "D1": [4.266, "млнт"], "D2": [1.774, "млнт"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Юильский 1', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
D1 - 4,0 млн.т
D2 - 1,3 млн.т
газ (извл.):
D2 - 1,3 млрд.м3', '{"oil": {"D1": [4.0, "млнт"], "D2": [1.3, "млнт"]}, "gas": {"D2": [1.3, "млрдм3"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Млечный', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (геол./извл.):
A - 0,464/0,133 млн.т
B1 - 0,556/0,194 млн.т
B2 - 2,784/0,852 млн.т', '{"oil": {"A": [0.464, "млнт"], "B1": [0.556, "млнт"], "B2": [2.784, "млнт"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Северо-Салымский - 3', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
D1 - 0,433 млн.т
D2 - 0,168 млн.т', '{"oil": {"D1": [0.433, "млнт"], "D2": [0.168, "млнт"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Приразломный-3', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.)
Д1 - 4,267 млн.т
Д2 - 1,658 млн.т', '{"oil": {"D1": [4.267, "млнт"], "D2": [1.658, "млнт"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Восточно-Айгульский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.)
Д0 - 1,717 млн.т
Д1 - 13,4 млн.т
Д2 - 0,7 млн.т
газ
Д1 - 2,9 млрд.м3
Д2 - 2,7 млрд.м3', '{"oil": {"D0": [1.717, "млнт"], "D1": [13.4, "млнт"], "D2": [0.7, "млнт"]}, "gas": {"D1": [2.9, "млрдм3"], "D2": [2.7, "млрдм3"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Восточно-Высотный', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.)
Д1 - 5,4 млн.т
Д2 - 0,1 млн.т', '{"oil": {"D1": [5.4, "млнт"], "D2": [0.1, "млнт"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Восточно-Заозерный', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.)
Д1 - 4,2 млн.т
Д2 - 0,4 млн.т
газ
Д2 - 0,3 млрд.м3', '{"oil": {"D1": [4.2, "млнт"], "D2": [0.4, "млнт"]}, "gas": {"D2": [0.3, "млрдм3"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Западно-Токушинский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.)
Д1 - 0,3 млн.т
Д1 - 0,1 млн.т
Д2 - 0,8 млн.т
газ (извл.)
Д1 - 5,1 млрд.м3
Д2 - 0,6 млрд.м3', '{"oil": {"D1": [0.1, "млнт"], "D2": [0.8, "млнт"]}, "gas": {"D1": [5.1, "млрдм3"], "D2": [0.6, "млрдм3"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Восточно-Толумский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.)
C1 - 0,156 млн.т
C2 - 0,814 млн.т
D0 - 0,919 млн.т', '{"oil": {"C1": [0.156, "млнт"], "C2": [0.814, "млнт"], "D0": [0.919, "млнт"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Восточно-Саранпаульский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.)
Д1 - 0,74 млн.т
газ
Д2 - 2,05 млрд.м3', '{"oil": {"D1": [0.74, "млнт"]}, "gas": {"D2": [2.05, "млрдм3"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Казымский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.)
Д0 - 14,006 млн.т
Д1 - 2,484 млн.т
Д2 - 17,255 млн.т
газ
Д1 - 0,783 млрд.м3
Д2 - 0,753 млрд.м3
конденсат (извл.)
Д1 - 0,023 млн.т
Д2 - 0,210 млн.т', '{"oil": {"D0": [14.006, "млнт"], "D1": [2.484, "млнт"], "D2": [17.255, "млнт"]}, "gas": {"D1": [0.783, "млрдм3"], "D2": [0.753, "млрдм3"]}, "cond": {"D1": [0.023, "млнт"], "D2": [0.21, "млнт"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Карымский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.)
Д1 - 1,2 млн.т.
Д2 - 0,3 млн.т.
газ
Д1 - 0,2 млрд.м3
Д2 - 0,4 млрд.м3', '{"oil": {"D1": [1.2, "млнт"], "D2": [0.3, "млнт"]}, "gas": {"D1": [0.2, "млрдм3"], "D2": [0.4, "млрдм3"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Кулингурский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл)
Д1 - 11,0 млн.т.
Д2 - 0,8 млн.т.
газ
Д1 - 1,3 млрд.м3
Д2 - 1,0 млрд.м3', '{"oil": {"D1": [11.0, "млнт"], "D2": [0.8, "млнт"]}, "gas": {"D1": [1.3, "млрдм3"], "D2": [1.0, "млрдм3"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Ляминский 20', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл)
Д1 - 14,0 млн.т
Д2 - 2,7 млн. т', '{"oil": {"D1": [14.0, "млнт"], "D2": [2.7, "млн т"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Малобыстринский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл)
Д1 - 11,5 млн.т
Д2 - 0,5 млн.т
газ
Д1 - 4,2 млрд.м3
Д2 - 1,3 млрд.м3', '{"oil": {"D1": [11.5, "млнт"], "D2": [0.5, "млнт"]}, "gas": {"D1": [4.2, "млрдм3"], "D2": [1.3, "млрдм3"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Малорогожниковский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.)
Дп - 2,1 млн.т
Д1 - 2,1 млн.т
Д2 - 0,1 млн.т', '{"oil": {"D1": [2.1, "млнт"], "D2": [0.1, "млнт"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Моимский 1', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
Д1 - 13,5 млн.т.
Д2 - 0,5 млн.т.
газ (извл.):
Д1 - 0,4 млрд. м3.
Д2 - 0,9 млрд. м3.', '{"oil": {"D1": [13.5, "млнт"], "D2": [0.5, "млнт"]}, "gas": {"D1": [0.4, "млрд м3"], "D2": [0.9, "млрд м3"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Моимский 2', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
Д1 - 11,5 млн.т.
Д2 - 0,5 млн.т.
газ (извл.):
Д1 - 0,3 млрд. м3.
Д2 - 0,8 млрд. м3.', '{"oil": {"D1": [11.5, "млнт"], "D2": [0.5, "млнт"]}, "gas": {"D1": [0.3, "млрд м3"], "D2": [0.8, "млрд м3"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Панлорский 2', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл)
Д1 - 8,2 млн.т
Д2 - 0,3 млн.т
газ
Д2 - 2,0 млрд.м3', '{"oil": {"D1": [8.2, "млнт"], "D2": [0.3, "млнт"]}, "gas": {"D2": [2.0, "млрдм3"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Похорский 2', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл)
Д1 - 5,0 млн.т.
Д2 - 0,8 млн.т.
газ
Д1 - 0,7 млрд. м3
Д2 - 1,4 млрд м3', '{"oil": {"D1": [5.0, "млнт"], "D2": [0.8, "млнт"]}, "gas": {"D1": [0.7, "млрд м3"], "D2": [1.4, "млрд м3"]}}'),
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
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Токушинский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.)
Д0 - 8,563 млн.т.
Д1 - 2,3 млн.т.
Д2 - 0,6 млн.т.
газ
Д1 - 2,5 млрд.м3
Д2 - 0,4 млрд.м3', '{"oil": {"D0": [8.563, "млнт"], "D1": [2.3, "млнт"], "D2": [0.6, "млнт"]}, "gas": {"D1": [2.5, "млрдм3"], "D2": [0.4, "млрдм3"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Шаймский 3', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.)
Д0 - 1.127 млн.т.
Д1 - 0.597 млн.т.
Д1 - 0.4 млн.т.
Д2 - 0.2 млн.т.', '{"oil": {"D0": [1.127, "млнт"], "D1": [0.4, "млнт"], "D2": [0.2, "млнт"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Шебурский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.)
Д0 - 1,080 млн.т.
Д1 - 0,5 млн.т.
Д1 - 1,0 млн.т.
Д2 - 0,3 млн.т.
газ
Д1 - 0,3 млрд.м3
Д2 - 0,8 млрд.м3', '{"oil": {"D0": [1.08, "млнт"], "D1": [1.0, "млнт"], "D2": [0.3, "млнт"]}, "gas": {"D1": [0.3, "млрдм3"], "D2": [0.8, "млрдм3"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Шугурский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.)
Д0 - 0,665 млн.т.
Д1 - 1,2 млн.т.
Д2 - 0,3 млн.т.
газ
Д1 - 0,2 млрд.м3
Д2 - 0,6 млрд.м3', '{"oil": {"D0": [0.665, "млнт"], "D1": [1.2, "млнт"], "D2": [0.3, "млнт"]}, "gas": {"D1": [0.2, "млрдм3"], "D2": [0.6, "млрдм3"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Южно-Айтульский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
Д0 - 3,760 млн.т.
Д1 - 5,786 млн.т.
Д2 - 10,0 млн.т.
Д2 - 0,7 млн.т.
газ (извл.):
Д1 - 5,0 млрд.м3.
Д2 - 3,9 млрд.м3.', '{"oil": {"D0": [3.76, "млнт"], "D1": [5.786, "млнт"], "D2": [0.7, "млнт"]}, "gas": {"D1": [5.0, "млрдм3"], "D2": [3.9, "млрдм3"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Южно-Серинский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.)
Д1 - 11.5 млн.т.
Д2 - 0.4 млн.т.
газ
Д1 - 0.4 млрд.м3
Д2 - 0.8 млрд.м3', '{"oil": {"D1": [11.5, "млнт"], "D2": [0.4, "млнт"]}, "gas": {"D1": [0.4, "млрдм3"], "D2": [0.8, "млрдм3"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Южно-Ташинский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.)
Д1 - 11.6 млн.т.
Д2 - 0.6 млн.т.', '{"oil": {"D1": [11.6, "млнт"], "D2": [0.6, "млнт"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Южно-Урманный', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.)
Д0 - 1.625 млн.т.
Д1 - 1.4 млн.т
Д2 - 0.1 млн.т', '{"oil": {"D0": [1.625, "млнт"], "D1": [1.4, "млнт"], "D2": [0.1, "млнт"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Южно-Ялотский', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
Д1 - 16,1 млн. т
Д2 - 0,9 млн. т
газ (изв.):
Д2 - 1,1 млрд. м3', '{"oil": {"D1": [16.1, "млн т"], "D2": [0.9, "млн т"]}, "gas": {"D2": [1.1, "млрд м3"]}}'),
(2026, '2026-01-02', 'Ханты-Мансийский автономный округ-Югра', e'Юильский 6', 'Прогнозный перечень участков недр углеводородного сырья, аукционы по которым планируется провести в 2026 году', e'https://rosnedra.gov.ru/activity/informatsionnye-resursy-i-programmy/prognoznyy-perechen-uchastkov-nedr-uglevodorodnogo-syrya-auktsiony-po-kotorym-planiruetsya-provesti-202602/', e'нефть (извл.):
Д1 - 1,2 млн. т
Д2 - 3,2 млн. т
газ (извл.):
Д2 - 3,0 млрд. м3', '{"oil": {"D1": [1.2, "млн т"], "D2": [3.2, "млн т"]}, "gas": {"D2": [3.0, "млрд м3"]}}');

CREATE OR REPLACE VIEW rosnedra.blocks_hcs_forecast_v
 AS
 WITH f_b_r AS (
         SELECT max(f.gid) AS gid,
            max(f.year) AS year,
            f.region,
            array_to_string(array_agg((f.block_name::text || chr(10)) || replace(f.resources_raw::text, chr(10), '. '::text)), (chr(10) || '--------------'::text) || chr(10)) AS blocks
           FROM rosnedra.blocks_hcs_forecast f
          WHERE f.year::double precision = date_part('year'::text, CURRENT_DATE)
          GROUP BY f.region
        )
 SELECT f_b_r.gid,
    f_b_r.year,
    f_b_r.region,
    f_b_r.blocks,
    r.geom
   FROM f_b_r
     JOIN hse."субъекты_россии" r ON r.region::text = f_b_r.region::text;