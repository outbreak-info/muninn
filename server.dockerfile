FROM python:3.11-bullseye
WORKDIR /usr/flu

EXPOSE 8000

COPY requirements.txt ./

RUN pip3 install -r requirements.txt

RUN echo '#! /bin/bash' > /bin/muninn_db_setup
RUN echo 'alembic upgrade head &>> /usr/flu/db_setup.log' >> /bin/muninn_db_setup
RUN echo 'python3 -u runinserts.py /usr/flu/test_data/SraRunTable_automated.csv sra_run_table_csv &>> /usr/flu/db_setup.log ' >> /bin/muninn_db_setup
RUN echo 'python3 -u runinserts.py /usr/flu/test_data/combined/combined_variants.tsv variants_tsv &>> /usr/flu/db_setup.log' >> /bin/muninn_db_setup
RUN echo 'python3 -u runinserts.py --kludge_mutations /usr/flu/test_data foo &>> /usr/flu/db_setup.log' >> /bin/muninn_db_setup
RUN echo 'python3 -u runinserts.py /usr/flu/test_data/eve_dms_scores.csv eve_dms_csv &>> /usr/flu/db_setup.log' >> /bin/muninn_db_setup
RUN echo 'python3 -u runinserts.py /usr/flu/test_data/HA_mouse_ferret_dms.tsv tmp_ha_mouse_ferret_dms &>> /usr/flu/db_setup.log' >> /bin/muninn_db_setup
RUN chmod u+x /bin/muninn_db_setup

ADD  alembic /usr/flu/alembic
ADD api /usr/flu/api
ADD DB /usr/flu/DB
ADD utils /usr/flu/utils
ADD parser /usr/flu/parser
COPY alembic.ini ./
COPY runinserts.py ./
ADD test_data/flu_db_test_data.tar.gz ./test_data/
