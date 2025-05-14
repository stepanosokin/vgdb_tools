FROM ghcr.io/osgeo/gdal:ubuntu-small-latest

WORKDIR /app/vgdb_tools/vgdb_bot

RUN apt-get update && apt-get install -y \
    software-properties-common
RUN add-apt-repository universe
RUN apt-get update && apt-get install -y \
    python3-pip \
    libpq-dev \
    python3-dev
RUN pip install --upgrade psycopg2 --break-system-packages
RUN pip install --upgrade requests --break-system-packages
RUN pip install --upgrade python-telegram-bot --break-system-packages
RUN pip install --upgrade fabric --break-system-packages
RUN pip install --upgrade paramiko --break-system-packages
RUN pip install --upgrade pymsteams --break-system-packages
RUN pip install --upgrade scp --break-system-packages
RUN pip install --upgrade python-dateutil --break-system-packages
RUN pip install --upgrade bs4 --break-system-packages
RUN pip install --upgrade pandas --break-system-packages
RUN pip install --upgrade openpyxl --break-system-packages
RUN pip install --upgrade xlrd --break-system-packages

COPY .pgdsn .
COPY .ext_pgdsn .
COPY bot_info_vgdb_bot_toAucGroup.json .
COPY bot_info_vgdb_bot_toStepan.json .
COPY .pggdal .
COPY license_blocks_general.webhook .
COPY 2024_blocks_nr_ne.webhook .
COPY 2024_blocks_np.webhook .
COPY .egssh .
COPY vgdb_torgi_gov_ru.py .
COPY vgdb_license_blocks_rfgf.py .
COPY synchro_evergis.py .
COPY vgdb_auctions_rosnedra.py .
COPY vgdb_general.py .
COPY vgdb_bot.py .

RUN mkdir rosnedra_auc
RUN mkdir rfgf_blocks
RUN mkdir torgi_gov_ru

RUN chmod 777 /app/vgdb_tools/vgdb_bot/rosnedra_auc
RUN chmod 777 /app/vgdb_tools/vgdb_bot/rfgf_blocks
RUN chmod 777 /app/vgdb_tools/vgdb_bot/torgi_gov_ru

CMD [ "python", "vgdb_bot.py" ]

# docker build -t stepanosokin/vgdb_bot -f ./vgdb_bot/Dockerfile .