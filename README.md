# Installation

```bash
docker run --name=azekyoo-tp-final --rm -p 8088:8088 -p 9870:9870 -p 9864:9864 -v ./data:/data -v ./jars:/jars -d hadoop-tp3:latest
```
```bash
docker exec -it azekyoo-tp-final bash
```
```bash
hdfs dfs -mkdir -p /relationships
```
```bash
hdfs dfs -put /data/relationships/data.txt /relationships/
```


# Job 1 - [.jar](jars/hadoop-tp3-collaborativeFiltering-job1-1.0.jar)

```bash
hadoop jar /jars/hadoop-tp3-collaborativeFiltering-job1-1.0.jar /data/relationships/data.txt /output/job1
```

# Job 2 - [.jar](jars/hadoop-tp3-collaborativeFiltering-job2-1.0.jar)

```bash
hadoop jar /jars/hadoop-tp3-collaborativeFiltering-job2-1.0.jar /output/job1 /output/job2
```

# Job 3 - [.jar](jars/hadoop-tp3-collaborativeFiltering-job3-1.0.jar)

```bash
hadoop jar /jars/hadoop-tp3-collaborativeFiltering-job3-1.0.jar /output/job2 /output/job3
```