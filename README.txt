PROJECT STRUCTURE:

	ny_bikeshare_analytics/
		├── app
		│   └── bikeshare_to_weather.py
		├── classes
		│   ├── __init__.py
		│   ├── sparkinit.py
		│   └── utilities.py
		├── configs
		│   └── config.json
		├── data
		│   ├── result_csv
		│   │   ├── part-00000-00751938-ab45-45ee-8dfa-5426e66ad38b-c000.csv
		│   │   └── _SUCCESS
		│   ├── result_parquet
		│   │   ├── part-00000-62544bc4-e606-4d95-819b-4ab22e9080ec-c000.snappy.parquet
		│   │   └── _SUCCESS
		│   ├── sample.csv
		│   └── weather_data_nyc_2016.csv
		├── __init__.py
		├── proto.ipynb
		├── README.txt
		├── starter.sh
		└── utilities.ipynb



>>> classes/sparkinit.py -> Blueprint for instantiating spark session uses `configs/config.json` for `app_name` and `master` configurations;

>>> classes/utilities.py -> Pseudo class with usefull functions for: 
						 -> json open(open_config); 
						 ->	parsing `date` for df join(parse_date); 
						 ->	checking if the inner join works fine(main df rows == join df rows  - handle_df_columns);
						 -> return schema for the analytics ready df; 
						 -> filter/constrains(preanalytics_filter);

>>> configs/config.json ->  Spark configuration for the session;

>>> data -> data/sample.csv: sample of data for different months(2016) from https://www.citibikenyc.com/system-data (Stream & History);
			data/weather_data_nyc_2016: https://www.kaggle.com/mathijs/weather-data-in-new-york-city-2016;
			data/result_csv: final df in csv format;
			data/result_parquet: final df in parquet;
			
>>> proto.ipynb -> the project prototyped in IPython notebook with dashboards in matplotlib;
>>> utilities.ipynb -> utilities.py in IPython notebook(imported in proto.ipynb to use for the functions);
>>> starter.sh -> single command for starting the project(date engineering only), can be set in cron, trigger, scheduler etc;
>>> requirements.txt -> used python modules in the project;

*DataFrame = df