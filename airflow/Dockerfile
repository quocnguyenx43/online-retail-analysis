FROM apache/airflow:3.0.4

# ENV HTTP_PROXY=http://alan1:xe&UvMTrrEOiPhA7@10.5.33.2:6777
# ENV HTTPS_PROXY=http://alan1:xe&UvMTrrEOiPhA7@10.5.33.2:6777

COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

COPY quarto.sh /
RUN cd / && bash /quarto.sh
