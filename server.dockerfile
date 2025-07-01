FROM python:3.13-bookworm

EXPOSE 8000

RUN useradd -Um muninn
WORKDIR /home/muninn

COPY --chown=muninn:muninn requirements.txt ./
RUN pip3 install -r requirements.txt

USER muninn

ADD --chown=muninn:muninn alembic ./alembic
ADD --chown=muninn:muninn api ./api
ADD --chown=muninn:muninn DB ./DB
ADD --chown=muninn:muninn utils ./utils
ADD --chown=muninn:muninn parser ./parser
COPY --chown=muninn:muninn alembic.ini ./
COPY --chown=muninn:muninn runinserts.py ./
COPY --chown=muninn:muninn containers/server/bin/* /bin/
