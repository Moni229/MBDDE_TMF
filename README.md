# MBDDE_TMF
TMF para el máster de Big Data &amp; Data Engineering


Estructura del repositorio: 
```
macroeconomy/
│
├── config/
│
├── sources/
│   ├── datasource.py
│   ├── fred_source.py
│   ├── yahoo_source.py
│   ├── eurostat_source.py
│   ├── finnhub_source.py
│   └── binance_source.py
│
├── readers/
│   ├── landing_reader.py
│   ├── bronze_reader.py
│   └── delta_reader.py
│
├── writers/
│   ├── landing_writer.py
│   ├── bronze_writer.py
│   ├── silver_writer.py
│   └── gold_writer.py
│
├── pipelines/
│   ├── landing/
│   │   ├── batch/
│   │   │   ├── run_fred.py
│   │   │   ├── run_yahoo.py
│   │   │   └── run_eurostat.py
│   │   │
│   │   └── streaming/
│   │       ├── run_finnhub.py
│   │       └── run_binance.py
│   │
│   ├── bronze/
│   │   ├── batch/
│   │   │   ├── fred_to_bronze.py
│   │   │   ├── yahoo_to_bronze.py
│   │   │   └── eurostat_to_bronze.py
│   │   │
│   │   └── streaming/
│   │       ├── finnhub_to_bronze.py
│   │       └── binance_to_bronze.py
│   │
│   ├── silver/
│   └── gold/
│
├── utils/
└── notebooks/
```