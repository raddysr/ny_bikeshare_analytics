PROJECT STRUCTURE:

	ny_bikeshares_analytics/
		├── app
		│   └── bikeshares_to_weather.py
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


>>> classes/sparkinit.py -> Blueprint for instantiating a spark session uses `configs/config.json` for app_namem, master configurations;

>>> classes/utilities.py -> Pseudo class with usefull functions for: 
						 -> json open(open_config) 
						 ->	parsing the date for df joining(parse_date), 
						 ->	checking if the join is OK(all the data is available - handle_df_columns)
						 -> return schema for the new df 
						 -> filter by constrains(preanalytics_filter)

>>> configs/config.json ->  Spark configuration for the session

>>> data -> data/sample.csv: sample data from different months(2016): https://www.citibikenyc.com/system-data (Stream & History)
			data/weather_data_nyc_2016: https://www.kaggle.com/mathijs/weather-data-in-new-york-city-2016
			data/result_csv: final dataframe in csv format		
			data/result_parquet: final dataframe in parquet

>>> proto.ipynb -> the project in IPython notebook with dashboards in matplotlib
>>> utilities.ipynb -> utilities.py in IPython notebook(imported in proto.ipynb to use for the functions)
>>> starter.sh -> single command for starting the project(date engineering)
>>> requirements.txt -> used modules in the project