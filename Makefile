.ONESHELL:

.default: run

list:
	uv run src/list.py --regions NIU --years 2024 --version 0.0.1

run:
	uv run src/run.py --tile-id 77,19 --year 2024 --version 0.0.1

build:
	docker build -t fc .

docker:
	docker build -t fc .
	docker run -it --rm fc uv run run.py --tile-id 50,41 --year 2024 --version 0.0.1

fmt:
	black src/
