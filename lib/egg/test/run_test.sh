#!/bin/bash
myvar="$PWD"
nameJob="TESTING"

  $SPARK_HOME/./bin/spark-submit --name "FiltersTesting" --packages com.databricks:spark-avro_2.10:1.0.0,com.databricks:spark-csv_2.10:1.0.3 \
 --jars $myvar/data-insights-jobs/experiments/francesco/app_v_3_0/lib/external_jars/aws-java-sdk-1.7.4.jar,$myvar/data-insights-jobs/experiments/francesco/app_v_3_0/lib/external_jars/avro-mapred-1.7.7.jar,$myvar/data-insights-jobs/experiments/francesco/app_v_3_0/lib/external_jars/hadoop-aws-2.7.1.jar, \
  --py-files $myvar/data-insights-jobs/experiments/francesco/app_v_3_0/lib/external_eggs/pidgin-2.1.0-py2.7-linux-x86_64.egg,$myvar/data-insights-jobs/experiments/francesco/app_v_3_0/lib/external_eggs/ghostrecon-1.0.0-py2.7-linux-x86_64.egg,$myvar/data-insights-jobs/experiments/francesco/app_v_3_0/lib/external_eggs/scikit_learn-0.18.dev0-py2.7-linux-x86_64.egg,$myvar/data-insights-jobs/experiments/francesco/app_v_3_0/lib/egg/dist/extra_functions-0.1-py2.7.egg \
  $myvar/data-insights-jobs/experiments/francesco/app_v_3_0/lib/egg/test/test_filters.py


  $SPARK_HOME/./bin/spark-submit --name "DisambiguationTesting" --packages com.databricks:spark-avro_2.10:1.0.0,com.databricks:spark-csv_2.10:1.0.3 \
 --jars $myvar/data-insights-jobs/experiments/francesco/app_v_3_0/lib/external_jars/aws-java-sdk-1.7.4.jar,$myvar/data-insights-jobs/experiments/francesco/app_v_3_0/lib/external_jars/avro-mapred-1.7.7.jar,$myvar/data-insights-jobs/experiments/francesco/app_v_3_0/lib/external_jars/hadoop-aws-2.7.1.jar, \
  --py-files $myvar/data-insights-jobs/experiments/francesco/app_v_3_0/lib/external_eggs/pidgin-2.1.0-py2.7-linux-x86_64.egg,$myvar/data-insights-jobs/experiments/francesco/app_v_3_0/lib/external_eggs/ghostrecon-1.0.0-py2.7-linux-x86_64.egg,$myvar/data-insights-jobs/experiments/francesco/app_v_3_0/lib/external_eggs/scikit_learn-0.18.dev0-py2.7-linux-x86_64.egg,$myvar/data-insights-jobs/experiments/francesco/app_v_3_0/lib/egg/dist/extra_functions-0.1-py2.7.egg \
  $myvar/data-insights-jobs/experiments/francesco/app_v_3_0/lib/egg/test/test_disambiguation.py


  $SPARK_HOME/./bin/spark-submit --name "FormattingTesting" --packages com.databricks:spark-avro_2.10:1.0.0,com.databricks:spark-csv_2.10:1.0.3 \
 --jars $myvar/data-insights-jobs/experiments/francesco/app_v_3_0/lib/external_jars/aws-java-sdk-1.7.4.jar,$myvar/data-insights-jobs/experiments/francesco/app_v_3_0/lib/external_jars/avro-mapred-1.7.7.jar,$myvar/data-insights-jobs/experiments/francesco/app_v_3_0/lib/external_jars/hadoop-aws-2.7.1.jar, \
  --py-files $myvar/data-insights-jobs/experiments/francesco/app_v_3_0/lib/external_eggs/pidgin-2.1.0-py2.7-linux-x86_64.egg,$myvar/data-insights-jobs/experiments/francesco/app_v_3_0/lib/external_eggs/ghostrecon-1.0.0-py2.7-linux-x86_64.egg,$myvar/data-insights-jobs/experiments/francesco/app_v_3_0/lib/external_eggs/scikit_learn-0.18.dev0-py2.7-linux-x86_64.egg,$myvar/data-insights-jobs/experiments/francesco/app_v_3_0/lib/egg/dist/extra_functions-0.1-py2.7.egg \
  $myvar/data-insights-jobs/experiments/francesco/app_v_3_0/lib/egg/test/test_formatting.py


  $SPARK_HOME/./bin/spark-submit --name "HelpersTesting" --packages com.databricks:spark-avro_2.10:1.0.0,com.databricks:spark-csv_2.10:1.0.3 \
 --jars $myvar/data-insights-jobs/experiments/francesco/app_v_3_0/lib/external_jars/aws-java-sdk-1.7.4.jar,$myvar/data-insights-jobs/experiments/francesco/app_v_3_0/lib/external_jars/avro-mapred-1.7.7.jar,$myvar/data-insights-jobs/experiments/francesco/app_v_3_0/lib/external_jars/hadoop-aws-2.7.1.jar, \
  --py-files $myvar/data-insights-jobs/experiments/francesco/app_v_3_0/lib/external_eggs/pidgin-2.1.0-py2.7-linux-x86_64.egg,$myvar/data-insights-jobs/experiments/francesco/app_v_3_0/lib/external_eggs/ghostrecon-1.0.0-py2.7-linux-x86_64.egg,$myvar/data-insights-jobs/experiments/francesco/app_v_3_0/lib/external_eggs/scikit_learn-0.18.dev0-py2.7-linux-x86_64.egg,$myvar/data-insights-jobs/experiments/francesco/app_v_3_0/lib/egg/dist/extra_functions-0.1-py2.7.egg \
  $myvar/data-insights-jobs/experiments/francesco/app_v_3_0/lib/egg/test/test_helpers.py


  $SPARK_HOME/./bin/spark-submit --name "SelectionTesting" --packages com.databricks:spark-avro_2.10:1.0.0,com.databricks:spark-csv_2.10:1.0.3 \
 --jars $myvar/data-insights-jobs/experiments/francesco/app_v_3_0/lib/external_jars/aws-java-sdk-1.7.4.jar,$myvar/data-insights-jobs/experiments/francesco/app_v_3_0/lib/external_jars/avro-mapred-1.7.7.jar,$myvar/data-insights-jobs/experiments/francesco/app_v_3_0/lib/external_jars/hadoop-aws-2.7.1.jar, \
  --py-files $myvar/data-insights-jobs/experiments/francesco/app_v_3_0/lib/external_eggs/pidgin-2.1.0-py2.7-linux-x86_64.egg,$myvar/data-insights-jobs/experiments/francesco/app_v_3_0/lib/external_eggs/ghostrecon-1.0.0-py2.7-linux-x86_64.egg,$myvar/data-insights-jobs/experiments/francesco/app_v_3_0/lib/external_eggs/scikit_learn-0.18.dev0-py2.7-linux-x86_64.egg,$myvar/data-insights-jobs/experiments/francesco/app_v_3_0/lib/egg/dist/extra_functions-0.1-py2.7.egg \
   $myvar/data-insights-jobs/experiments/francesco/app_v_3_0/lib/egg/test/test_selection.py


   $SPARK_HOME/./bin/spark-submit --name "SentimentTesting" --packages com.databricks:spark-avro_2.10:1.0.0,com.databricks:spark-csv_2.10:1.0.3 \
 --jars $myvar/data-insights-jobs/experiments/francesco/app_v_3_0/lib/external_jars/aws-java-sdk-1.7.4.jar,$myvar/data-insights-jobs/experiments/francesco/app_v_3_0/lib/external_jars/avro-mapred-1.7.7.jar,$myvar/data-insights-jobs/experiments/francesco/app_v_3_0/lib/external_jars/hadoop-aws-2.7.1.jar, \
  --py-files $myvar/data-insights-jobs/experiments/francesco/app_v_3_0/lib/external_eggs/pidgin-2.1.0-py2.7-linux-x86_64.egg,$myvar/data-insights-jobs/experiments/francesco/app_v_3_0/lib/external_eggs/ghostrecon-1.0.0-py2.7-linux-x86_64.egg,$myvar/data-insights-jobs/experiments/francesco/app_v_3_0/lib/external_eggs/scikit_learn-0.18.dev0-py2.7-linux-x86_64.egg,$myvar/data-insights-jobs/experiments/francesco/app_v_3_0/lib/egg/dist/extra_functions-0.1-py2.7.egg \
  $myvar/data-insights-jobs/experiments/francesco/app_v_3_0/lib/egg/test/test_sentiment.py


   $SPARK_HOME/./bin/spark-submit --name "LoadTesting" --packages com.databricks:spark-avro_2.10:1.0.0,com.databricks:spark-csv_2.10:1.0.3 \
 --jars $myvar/data-insights-jobs/experiments/francesco/app_v_3_0/lib/external_jars/aws-java-sdk-1.7.4.jar,$myvar/data-insights-jobs/experiments/francesco/app_v_3_0/lib/external_jars/avro-mapred-1.7.7.jar,$myvar/data-insights-jobs/experiments/francesco/app_v_3_0/lib/external_jars/hadoop-aws-2.7.1.jar, \
  --py-files $myvar/data-insights-jobs/experiments/francesco/app_v_3_0/lib/external_eggs/pidgin-2.1.0-py2.7-linux-x86_64.egg,$myvar/data-insights-jobs/experiments/francesco/app_v_3_0/lib/external_eggs/ghostrecon-1.0.0-py2.7-linux-x86_64.egg,$myvar/data-insights-jobs/experiments/francesco/app_v_3_0/lib/external_eggs/scikit_learn-0.18.dev0-py2.7-linux-x86_64.egg,$myvar/data-insights-jobs/experiments/francesco/app_v_3_0/lib/egg/dist/extra_functions-0.1-py2.7.egg \
  $myvar/data-insights-jobs/experiments/francesco/app_v_3_0/lib/egg/test/test_load.py


