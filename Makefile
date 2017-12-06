
MODEL = GBTRegression


MODE = evaluation
MODE = training

CSV_NAME = "dataset_680k.csv"

JAR_NAME = "$(MODEL)_$(MODE)_$(CSV_NAME).jar"

all: build run

build:
	sbt compile
	sbt package
	cp target/scala-*/*.jar $(JAR_NAME)

run:
	spark-submit  --master local[*]   --class $(MODEL)  $(JAR_NAME)  data  model_saved $(MODE) $(CSV_NAME)

clean:
	rm *.jar


