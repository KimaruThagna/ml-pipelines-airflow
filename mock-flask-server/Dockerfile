FROM python:3.8.3-alpine

RUN mkdir -p code
WORKDIR code
COPY /app .

RUN pip install -r requirements.txt

ENTRYPOINT python app.py