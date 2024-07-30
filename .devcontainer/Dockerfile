# Use an official GDAL image as the base image
FROM ghcr.io/osgeo/gdal:ubuntu-full-latest

WORKDIR /home/evergis_tools

COPY requirements.txt ./

RUN apt-get update && apt-get install -y \
    software-properties-common
RUN add-apt-repository universe
RUN apt-get update && apt-get install -y \
    python3-pip \
    python3-dev
RUN apt-get update && apt-get -y install libpq-dev gcc 
# RUN pip install -r requirements.txt
# RUN apt-get update && apt-get -y install libpq-dev gcc && pip install psycopg2


# # установка debian-пакетов в среде Linux
# # работает только для Debian-based пакетов, например не работает для pymsteams
RUN apt-get update && apt-get -y install python3-psycopg2
RUN apt-get update && apt-get -y install python3-requests
RUN apt-get update && apt-get -y install python3-fabric
RUN apt-get update && apt-get -y install python3-python-telegram-bot
RUN apt-get update && apt-get -y install python3-bs4
RUN apt-get update && apt-get -y install python3-pandas
RUN apt-get update && apt-get -y install python3-openpyxl
RUN apt-get update && apt-get -y install python3-xlrd

# # Пробуем создать отдельное окружение в контейнере. не получилось.
# RUN apt-get update && apt-get -y install python3-venv
# RUN python3 -m venv venv
# RUN source venv/bin/activate
# RUN pip instal psycopg2

# RUN apt-get update && apt-get -y install python3-pymsteams
# RUN apt-get update && apt-get -y install python3-socket
# RUN python3.10 -m pip install psycopg2
# RUN pip install psycopg2-binary
# RUN python get-pip.py
# RUN python3-pip install psycopg2
# RUN pip install requests
# RUN pip install fabric --upgrade
# RUN pip install python-telegram-bot --upgrade
# RUN pip install bs4
# RUN pip install pandas
# RUN pip install openpyxl
# RUN pip install xlrd
# RUN pip install pymsteams
# RUN pip install -r requirements.txt
# RUN pip install socket

COPY . .

# CMD [ "python", "./hello_world.py" ]