FROM python:3.11-bullseye
WORKDIR /usr/flu

EXPOSE 8000

COPY requirements.txt ./

RUN pip3 install -r requirements.txt

ADD  alembic /usr/flu/alembic
ADD api /usr/flu/api
ADD DB /usr/flu/DB
ADD utils /usr/flu/utils
ADD parser /usr/flu/parser
COPY alembic.ini ./
COPY runinserts.py ./
ADD test_data/flu_db_test_data.tar.gz ./test_data/
