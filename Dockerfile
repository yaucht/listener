FROM python:3.8.6-slim-buster

RUN mkdir /app

COPY ./app/requirements.txt /app

RUN pip install --no-cache-dir --quiet -r /app/requirements.txt && \
    pip install --no-cache-dir --quiet uvicorn

EXPOSE 80

COPY ./app /app/

ENTRYPOINT ["uvicorn"]
CMD ["app.main:app", "--host", "0.0.0.0", "--port", "80"]
