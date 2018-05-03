# Phenomenon Detector

## Required Software Installation

1. Scala 2.11
`brew install scala@2.11`
2. SBT Builder
`brew install sbt`
3. Apache Spark 2.3.0
`brew install apache-spark`

## How to run?

`4` in `local[4]` is the core number that you want to allocate to the application.

```
sbt package
spark-submit \
  --class "Main" \
  --master "local[4]" \
  target/scala-2.11/phenomenon-detector_2.11-1.0.jar
```

You will see the output in the `output.txt` file.
