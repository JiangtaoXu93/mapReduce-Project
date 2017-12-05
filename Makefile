#SPARK_HOME = /usr/local/Cellar/apache-spark/2.2.0/libexec/
JAR_NAME = GBTRegression400K.jar

all: build run

build:
	sbt compile
	sbt package
	cp target/scala-*/*.jar $(JAR_NAME)

run:
	spark-submit  --master local[*]   --class GBTRegression  $(JAR_NAME)  data    model_saved



