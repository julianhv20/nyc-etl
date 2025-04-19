import os

# Configuración de rutas
BASE_PATH = os.path.dirname(os.path.abspath(__file__))
RAW_LAYER_PATH = os.path.join(BASE_PATH, "data", "raw")
TRUSTED_LAYER_PATH = os.path.join(BASE_PATH, "data", "trusted")
REFINED_LAYER_PATH = os.path.join(BASE_PATH, "data", "refined")

# Configuración de datos
SOURCE_PATH = "s3://nyc-tlc/yellow/"  # Cambiar según la fuente elegida
YEARS_TO_PROCESS = [2020, 2021]  # Años representativos
LOOKUP_CSV_PATH = os.path.join(BASE_PATH, "data", "trusted", "taxi_zone_lookup.csv")