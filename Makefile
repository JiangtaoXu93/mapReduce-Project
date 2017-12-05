#SPARK_HOME = /usr/local/Cellar/apache-spark/2.2.0/libexec/
JAR_NAME = project.jar

all: build run

build:
	sbt compile
	sbt package
	cp target/scala-*/*.jar $(JAR_NAME)

run:
	#configuration for C5/C5 xLARGE
	spark-submit  --master local[*]   --class GBTRegression  $(JAR_NAME)  data     model_saved



