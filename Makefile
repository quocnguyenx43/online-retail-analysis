# Setup Containers

hdfs:
	docker compose -f ./hdfs/docker-compose.yaml up -d

hdfs-down:
	docker compose -f ./hdfs/docker-compose.yaml down -v

hdfs-client:
	docker compose -f ./docker-compose.yaml up -d hdfs-client

airflow:
	docker compose -f docker-compose.yaml up -d

airflow-down:
	docker compose -f docker-compose.yaml down -v

pytest:
	docker exec webserver pytest -p no:warnings -v /opt/airflow/tests

distclean:
	rm -rf ./hdfs/data/nameNode/* ./hdfs/data/dataNode/* ./hdfs/data/secondaryNameNode/*

.PHONY: hdfs hdfs-down hdfs-client airflow airflow-down distclean pytest
