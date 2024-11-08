WAYANG_BIN := ./wayang-0.7.1/bin/wayang-submit
DATA_PATH := /home/mlflexer/repos/ADS/assignments/as3/python/data
RESULTS_PATH := /home/mlflexer/repos/ADS/assignments/as3/results/

gen_type := str
gen_file := GeneratedString

compile:
	mvn clean install -DskipTests -Drat.skip=true
	mvn clean package -pl :wayang-assembly -Pdistribution
	tar -xvf ./wayang-assembly/target/apache-wayang-assembly-0.7.1-incubating-dist.tar.gz

wordcount:
	$(WAYANG_BIN) org.apache.wayang.apps.parquet.Main $(DATA_PATH)/yelp_reviews_700.parquet $(RESULTS_PATH)wordcount.csv
	$(WAYANG_BIN) org.apache.wayang.apps.parquet.WordCountNoProjection $(DATA_PATH)/yelp_reviews_700.parquet $(RESULTS_PATH)wordcount.csv
	$(WAYANG_BIN) org.apache.wayang.apps.parquet.Csv file://$(DATA_PATH)/yelp_reviews_700.csv $(RESULTS_PATH)wordcount.csv
	$(WAYANG_BIN) org.apache.wayang.apps.parquet.Main $(DATA_PATH)/yelp_reviews_7000.parquet $(RESULTS_PATH)wordcount.csv
	$(WAYANG_BIN) org.apache.wayang.apps.parquet.WordCountNoProjection $(DATA_PATH)/yelp_reviews_7000.parquet $(RESULTS_PATH)wordcount.csv
	$(WAYANG_BIN) org.apache.wayang.apps.parquet.Csv file://$(DATA_PATH)/yelp_reviews_7000.csv $(RESULTS_PATH)wordcount.csv
	$(WAYANG_BIN) org.apache.wayang.apps.parquet.Main $(DATA_PATH)/yelp_reviews_70000.parquet $(RESULTS_PATH)wordcount.csv
	$(WAYANG_BIN) org.apache.wayang.apps.parquet.WordCountNoProjection $(DATA_PATH)/yelp_reviews_70000.parquet $(RESULTS_PATH)wordcount.csv
	$(WAYANG_BIN) org.apache.wayang.apps.parquet.Csv file://$(DATA_PATH)/yelp_reviews_70000.csv $(RESULTS_PATH)wordcount.csv
	$(WAYANG_BIN) org.apache.wayang.apps.parquet.Main $(DATA_PATH)/yelp_reviews_700000.parquet $(RESULTS_PATH)wordcount.csv
	$(WAYANG_BIN) org.apache.wayang.apps.parquet.WordCountNoProjection $(DATA_PATH)/yelp_reviews_700000.parquet $(RESULTS_PATH)wordcount.csv
	$(WAYANG_BIN) org.apache.wayang.apps.parquet.Csv file://$(DATA_PATH)/yelp_reviews_700000.csv $(RESULTS_PATH)wordcount.csv

gen:
	$(WAYANG_BIN) org.apache.wayang.apps.parquet.$(gen_file) $(DATA_PATH)/$(gen_type)_10_10000.parquet $(RESULTS_PATH)$(gen_type)_10.csv
	$(WAYANG_BIN) org.apache.wayang.apps.parquet.Csv$(gen_file) file://$(DATA_PATH)/$(gen_type)_10_10000.csv $(RESULTS_PATH)$(gen_type)_10.csv
	$(WAYANG_BIN) org.apache.wayang.apps.parquet.$(gen_file) $(DATA_PATH)/$(gen_type)_10_100000.parquet $(RESULTS_PATH)$(gen_type)_10.csv
	$(WAYANG_BIN) org.apache.wayang.apps.parquet.Csv$(gen_file) file://$(DATA_PATH)/$(gen_type)_10_100000.csv $(RESULTS_PATH)$(gen_type)_10.csv
	$(WAYANG_BIN) org.apache.wayang.apps.parquet.$(gen_file) $(DATA_PATH)/$(gen_type)_10_1000000.parquet $(RESULTS_PATH)$(gen_type)_10.csv
	$(WAYANG_BIN) org.apache.wayang.apps.parquet.Csv$(gen_file) file://$(DATA_PATH)/$(gen_type)_10_1000000.csv $(RESULTS_PATH)$(gen_type)_10.csv
	$(WAYANG_BIN) org.apache.wayang.apps.parquet.$(gen_file) $(DATA_PATH)/$(gen_type)_10_10000000.parquet $(RESULTS_PATH)$(gen_type)_10.csv
	$(WAYANG_BIN) org.apache.wayang.apps.parquet.Csv$(gen_file) file://$(DATA_PATH)/$(gen_type)_10_10000000.csv $(RESULTS_PATH)$(gen_type)_10.csv

gen_col:
	$(WAYANG_BIN) org.apache.wayang.apps.parquet.$(gen_file) $(DATA_PATH)/$(gen_type)_1_10000000.parquet $(RESULTS_PATH)$(gen_type)_10000000.csv
	$(WAYANG_BIN) org.apache.wayang.apps.parquet.Csv$(gen_file) file://$(DATA_PATH)/$(gen_type)_1_10000000.csv $(RESULTS_PATH)$(gen_type)_10000000.csv
	$(WAYANG_BIN) org.apache.wayang.apps.parquet.$(gen_file) $(DATA_PATH)/$(gen_type)_2_10000000.parquet $(RESULTS_PATH)$(gen_type)_10000000.csv
	$(WAYANG_BIN) org.apache.wayang.apps.parquet.Csv$(gen_file) file://$(DATA_PATH)/$(gen_type)_2_10000000.csv $(RESULTS_PATH)$(gen_type)_10000000.csv
	$(WAYANG_BIN) org.apache.wayang.apps.parquet.$(gen_file) $(DATA_PATH)/$(gen_type)_3_10000000.parquet $(RESULTS_PATH)$(gen_type)_10000000.csv
	$(WAYANG_BIN) org.apache.wayang.apps.parquet.Csv$(gen_file) file://$(DATA_PATH)/$(gen_type)_3_10000000.csv $(RESULTS_PATH)$(gen_type)_10000000.csv
	$(WAYANG_BIN) org.apache.wayang.apps.parquet.$(gen_file) $(DATA_PATH)/$(gen_type)_4_10000000.parquet $(RESULTS_PATH)$(gen_type)_10000000.csv
	$(WAYANG_BIN) org.apache.wayang.apps.parquet.Csv$(gen_file) file://$(DATA_PATH)/$(gen_type)_4_10000000.csv $(RESULTS_PATH)$(gen_type)_10000000.csv
	$(WAYANG_BIN) org.apache.wayang.apps.parquet.$(gen_file) $(DATA_PATH)/$(gen_type)_5_10000000.parquet $(RESULTS_PATH)$(gen_type)_10000000.csv
	$(WAYANG_BIN) org.apache.wayang.apps.parquet.Csv$(gen_file) file://$(DATA_PATH)/$(gen_type)_5_10000000.csv $(RESULTS_PATH)$(gen_type)_10000000.csv
	$(WAYANG_BIN) org.apache.wayang.apps.parquet.$(gen_file) $(DATA_PATH)/$(gen_type)_6_10000000.parquet $(RESULTS_PATH)$(gen_type)_10000000.csv
	$(WAYANG_BIN) org.apache.wayang.apps.parquet.Csv$(gen_file) file://$(DATA_PATH)/$(gen_type)_6_10000000.csv $(RESULTS_PATH)$(gen_type)_10000000.csv
	$(WAYANG_BIN) org.apache.wayang.apps.parquet.$(gen_file) $(DATA_PATH)/$(gen_type)_7_10000000.parquet $(RESULTS_PATH)$(gen_type)_10000000.csv
	$(WAYANG_BIN) org.apache.wayang.apps.parquet.Csv$(gen_file) file://$(DATA_PATH)/$(gen_type)_7_10000000.csv $(RESULTS_PATH)$(gen_type)_10000000.csv
	$(WAYANG_BIN) org.apache.wayang.apps.parquet.$(gen_file) $(DATA_PATH)/$(gen_type)_8_10000000.parquet $(RESULTS_PATH)$(gen_type)_10000000.csv
	$(WAYANG_BIN) org.apache.wayang.apps.parquet.Csv$(gen_file) file://$(DATA_PATH)/$(gen_type)_8_10000000.csv $(RESULTS_PATH)$(gen_type)_10000000.csv
	$(WAYANG_BIN) org.apache.wayang.apps.parquet.$(gen_file) $(DATA_PATH)/$(gen_type)_9_10000000.parquet $(RESULTS_PATH)$(gen_type)_10000000.csv
	$(WAYANG_BIN) org.apache.wayang.apps.parquet.Csv$(gen_file) file://$(DATA_PATH)/$(gen_type)_9_10000000.csv $(RESULTS_PATH)$(gen_type)_10000000.csv
	$(WAYANG_BIN) org.apache.wayang.apps.parquet.$(gen_file) $(DATA_PATH)/$(gen_type)_10_10000000.parquet $(RESULTS_PATH)$(gen_type)_10000000.csv
	$(WAYANG_BIN) org.apache.wayang.apps.parquet.Csv$(gen_file) file://$(DATA_PATH)/$(gen_type)_10_10000000.csv $(RESULTS_PATH)$(gen_type)_10000000.csv

