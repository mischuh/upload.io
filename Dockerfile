FROM python:3.7.0-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends bash git

ADD . ${WORKDIR}
RUN pip3 install --no-cache-dir -r /app/requirements.txt

RUN cd ${WORKDIR} && mkdir -p /watchdog

CMD ["python3",  "./runner.py", "-p", "/watchdog", "-c", "resources/catalog_auszug.json", "-s", "kontoauszug"]