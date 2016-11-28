# Language Pipeline
The language pipeline can be used to query Switfkey's language data. It can perform several types of analysis including co-occurrence, trends, stats and create a list of users or install ids that meet certain criteria. It can be combined easily with external datasets (telemetry, customozied parquet and csv files, etc.) and it can augment further the language data by adding attributes both from avro and external sources.

So far five different analysis are supported:
* co_occurrence - words or sentences reoccurring with certain keywords or conditions
* users_list -  list of users meeting certain requirements. Attributes can be added to each user
* installids_list - list of installids meeting certain requirements. Attributes can be added to each installId
* stats - stats wit number of users and number of words
* trends - similar to co_occurrence but it only selects the keywords
* sentiment - uses "negative" average wordvecs and "positive" averaged wordvecs to calculate the sentiment for each sentence

COMING SOON:
* topic modelling
* clustering

## How To Run It
First of all you need to set SPARK_HOME (the folder where Spark is), AWS_SECRET_ACCESS_KEY and AWS_ACCESS_KEY_ID in your .bashrc folder.

In order to run the script all you need to do is the following, given that you have set up the enviroment variable SPARK_HOME to your spark directory and you are in the same directory where app_v_3_0 is saved:

```
bash path/to/run_pipeline path/to/the/json/config/file
```

Some fine tuning with Spark might be required depending on the size of the data. 

If you would like to use the emailing functionality enter in SPARK_HOME/conf/spark-env.sh the OUTLOOK_EMAIL and OUTLOOK_PASSWORD env variable.

## Structure Of A Config File
A JSON config file is passed from the command using `--file`. The parameters can be divided into global and local paraments, where the global ones affect all the queries while the local ones are just for one specific query.

The global parameters are:
* num_fragments - it's a percentage. it can be used to specify the portion of data that we want to query. If it is not specified then it will run over the whole dataset
* input_folder - used to point to the input dataset
* output_folder - used to specify the output location where the results will be saved
* output_email - string of emails divided by comma to where the results will be sent. If it is not specified Spark will still save the results but it will not email them
* nltk_tokenizer - if set to "true" the nltk tokenizer will be used which is more accurate but slower
* stopwords - path to a csv file containing additional stopwords 

Each query has local parameters that can be set up:
* name - the name that will be used to save the results
* query - a list where it is possible to define which keyowrds to look for
* filters - OPTIONAL: a dictionary where to define the filtering parameters for each query. Filtering is further explained below
* attributes - OPTIONAL: a dictionary where to define the attributes to be added to the results. Attributes are further explained below
* type_of_analysis - it can be either co_occurrence, users_list, installids_list, trends, sentiment or stats for the moment 
* disambiguation_keywords - OPTIONAL: a list of words that can define a context to distinguish the context in which we want the query to be. Example: if you want to get co-occurrent words for Harrison Ford and we define Ford as a query then we can add here a list such as "cinema", "movie", "oscars" in order to disitnguish it from the car brand

Possible ways to write the keywords in "query":
* case 1: "bernie sanders" - it checks if "bernie" and "sanders" are contained in the reconstructed sentence with no precise order
* case 2: "bernie_sanders" - it checks if "bernie" and "sanders" are contained in the reconstructed sentence in the given order
* case 3: "regex:bern" - it checks if there is a word that matches the pattern "bern"

Paramaters used in filtering:
* start_date - starting date from where to start querying the data. It has to be in this format: dd/mm/yyyy
* end_date - last date used for the query. It has to be in this format: dd/mm/yyyy
* ngrams - length of the ngram to be returned. Default is 1. If interested in whole sentence "ngram":"all"
* emoji_sentence - if set to "true" it will filter all the sentences containing emojis
* all_dataset - if set to "true" it will run through the dataset without considering specific keywords
* users_limit - users threshold below which the query is filtered. Default value is 25
* freq_limit - frequency threshold below which the query is filtered. Default value is 25
* users - path to a csv file containing a selected group of users
* installIds - path to a csv file containing a selected group of installIds
* other fields: it is possible to address specific fields as far as they are valid fields of the original dataset. Example: "geoIP.country":["US", "IN", "MX"]. See the folder "schemas" for an idea of what fields you can find in telemetry and in the language data.
* extras - a list of dictionaries. It can be used to define external datasets that be used for filtering
	Keys for extras:
		* format - for now it supports only "parquet" and "csv"
		* identifier - a common identifier used between the language data and the external dataset. It can be a cloudUserId or an installId.  It can either a string or an integer (if using csv)
		* path - location of the external dataset
		* key - name of the field that needs to be filtered. It can either a string or an integer (if using csv)
		* values - list of desired values

Parameters used in attributes:
* time_division - available choices are "months","weeks","days" and "years"
* gender - it associates the gender to each cloud user
* other fields: it is possible to address specific fields as far as they are valid fields of the original dataset. Example: "geoIP.country": "true"
* extras - a list of dictionaries. It can be used to add attributes from define external datasets
	Keys for extras:
		* format - for now it supports only "parquet" and "csv"
		* path - location of the external dataset
		* key - common identifier between language data and external dataset (cloudUserId or installId). It can either a string or an integer (if using csv)
		* values - list of desired attributes to add.  It can either a string or an integer (if using csv)


## Architecture

The pipeline is structured in several different stages:

1. Pre-processing -  all the queries are aggregated together and the minimum starting date and the maximum ending date are extracted from the queries. The whole dataset is then filtered by queries and by time and cached.
2. First Stage Filter - the data is filtered for each query 
3. Formatting - the attributes are added to each query
4. Second Stage Filtering - stopwords are removed and thresholding is applied

## How To Launch The Python Server And Send Requests From Outside The Bastion

Firstly we need to start the server on the bastion:
```
python web_app.py
```

The server will be listening at the following address: http://mesos-insights-bastion-b-1.api.swiftkey.com:4048/api . An easy and recommended way to send POST requests is to use a Google Chrome extension such as ARC (Advance Rest Client) since the app doesn't have a UI yet. 

Send a json POST reuqest using the exact same syntax used in the normal config files. The json will be converted to a file and passed to the Python Language Pipeline (Ferrari Pipeline for friends).

## Testing

For testing just run the following script from $SPARK_HOME:
```
bash data-insights-jobs/experiments/francesco/app_v_3_0/lib/egg/test/run_test.sh
```
