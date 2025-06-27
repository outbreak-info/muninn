FROM python:3.13-bookworm
WORKDIR /flu

EXPOSE 8000

COPY requirements.txt ./

RUN pip3 install -r requirements.txt

ADD  alembic /flu/alembic
ADD api /flu/api
ADD DB /flu/DB
ADD utils /flu/utils
ADD parser /flu/parser
COPY alembic.ini ./
COPY runinserts.py ./
COPY containers/server/bin/* /bin/
