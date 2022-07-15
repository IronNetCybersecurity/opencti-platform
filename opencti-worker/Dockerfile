FROM python:3.9.12-alpine3.15

WORKDIR /opt/opencti-worker

COPY src/requirements.txt .

RUN apk --no-cache add git build-base libmagic libffi-dev \
    && pip3 install --no-cache-dir --requirement requirements.txt \
    && pip3 install \
        --upgrade \
        --force \
        --no-cache-dir \
        git+https://github.com/rlynch-ironnet/client-python@vhost \
    && apk del git build-base

COPY src /opt/opencti-worker

ENTRYPOINT ["python3"]
CMD ["worker.py"]
