# Define your column mapping
column_mapping_sales = {
    'REF_DATE': 'date_sales',
    'GEO': 'name_province',
    'DGUID': 'id_province',
    'Seasonal adjustment': 'name_seasonal_adjustment',
    'North American Industry Classification System (NAICS)': 'name_naics',
    'VALUE': 'value',
}

column_mapping_seasonal_adjustments = {
    'name_seasonal_adjustment': 'name'
}

column_mapping_province = {
    'id_province': 'id',
    'name_province': 'name'
}

column_mapping_naics = {
    'id_naics': 'id',
    'name_naics': 'name'
}

dim_province_schema = [
    ("id","INT PRIMARY KEY"),
    ("name","VARCHAR(50)"),
    ("latitude","VARCHAR(20)"),
    ("longitude","VARCHAR(20)")
]

dim_naics_schema = [
    ("id","VARCHAR(10) PRIMARY KEY"),
    ("name","VARCHAR(100)")
]

fact_sales_schema = [
    ("date_sales", "DATE"),
    ("id_province","INT REFERENCES dim_province(id)"),
    ("id_seasonal","INT REFERENCES dim_seasonal_adjustment(id)"),
    ("id_naics",   "VARCHAR(10) REFERENCES dim_naics(id)"),
    ("value", "double precision")
]

dim_file_log_schema = [
    ("id","SERIAL PRIMARY KEY"),
    ("date_sales","VARCHAR(7)"),
    ("file_name","VARCHAR(100)"),
    ("file_date","TIMESTAMP"),
    ("status","VARCHAR(20)")
]

dim_seasonal_adjustment_schema = [
    ("id","INT PRIMARY KEY"),
    ("name","VARCHAR(50)")
]
