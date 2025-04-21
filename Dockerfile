FROM python:3.12-slim

WORKDIR /app

ENV TZ=Asia/Shanghai
VOLUME ["/config", "/logs", "/media"]

COPY requirements.txt requirements.txt

RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt \
    && rm -rf requirements.txt

COPY app /app

ENTRYPOINT ["python", "/app/main.py"]
