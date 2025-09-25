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
	find ./hdfs/data/nameNode     -mindepth 1 ! -name '.gitkeep' -delete
	find ./hdfs/data/dataNode     -mindepth 1 ! -name '.gitkeep' -delete
	find ./hdfs/data/secondaryNameNode -mindepth 1 ! -name '.gitkeep' -delete

.PHONY: hdfs hdfs-down hdfs-client airflow airflow-down distclean pytest
