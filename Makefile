# input file for query
QUERY_FILE = input.csv

# file used as our database
CSV_NAME = dataset_820k.csv

JAR_NAME = model-YangXia_JiangtaoXu_YuWen.jar


#jar name for GBTRegression
GBT_MAXITER = 10
GBT_MAXDEPTH = 10
#JAR_NAME = "$(MODEL)_$(MODE)_$(CSV_NAME)_maxIter$(GBT_MAXITER)_maxDepth$(GBT_MAXDEPTH).jar"

#jar name for RandomForestRegression
RDREG_NUMTREE = 100
RDREG_MAXDEPTH = 15
#JAR_NAME = "$(MODEL)_$(MODE)_$(CSV_NAME)_numTree$(RDREG_NUMTREE)_maxDepth$(RDREG_MAXDEPTH).jar"


#MODE training or evaluation; invalid option when calling predict
MODE = evaluation

# feasible model is GBTRegression LinearRegression RandomForestRegression
MODEL = RandomForestRegression



all: build predict

build:
	sbt compile
	sbt package
	cp target/scala-*/*.jar $(JAR_NAME)

predict:
	spark-submit  --master local[*]   --class neu.pdpmr.project.Model $(JAR_NAME) $(QUERY_FILE) $(CSV_NAME)
	mv temp/part-*.csv  output.csv
	rm -rf temp

train:
	spark-submit  --master local[*]   --class $(MODEL) --driver-memory 8g $(JAR_NAME)  data  model_saved $(MODE) $(CSV_NAME)

clean:
	rm *.jar









