# Makefile de utilidades del proyecto (desarrollo local y tareas de orquestación simples).
# Requiere Python y, para Spark en Docker, Docker Desktop instalado.

.PHONY: help setup run spark-docker spark-docker-up spark-docker-down

help:
	@echo "Targets disponibles: setup | run | spark-docker | spark-docker-up | spark-docker-down"

setup:
	python -m pip install -r requirements.txt

run:
	python run_pipeline.py

# Ejecución Spark dentro de contenedor (Windows/Linux/Mac)
spark-docker:
	[ -f .env.docker ] || cp .env .env.docker
	docker compose -f docker-compose.spark.yml up --build --abort-on-container-exit

spark-docker-up:
	[ -f .env.docker ] || cp .env .env.docker
	docker compose -f docker-compose.spark.yml up --build

spark-docker-down:
	docker compose -f docker-compose.spark.yml down -v
