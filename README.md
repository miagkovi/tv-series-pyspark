# tv-series-pyspark
## PySpark educational project

Put tvs.json into data/.

Create an image:

```bash
docker build -t spark-app .
```

Run the container:

```bash
docker run --rm spark-app
```

Docker-compose:

```bash
docker-compose up --build
```