FROM apache/spark:3.5.0

USER root
COPY main.py /opt/spark/work-dir/main.py
COPY data/tvs.json /opt/spark-data/tvs.json

WORKDIR /opt/spark/work-dir

CMD ["/opt/spark/bin/spark-submit", "--master", "local[*]", "main.py"]