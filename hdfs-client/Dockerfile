FROM python:3.12-slim

WORKDIR /app

COPY ./requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

COPY ./upload-data-to-datalake.py /app/upload-data-to-datalake.py

CMD ["python3", "upload-data-to-datalake.py"]
