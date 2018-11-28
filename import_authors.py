import csv
import json
import pandas as pd
import json
import geocoder
from sqlalchemy import create_engine
from pprint import pprint

codes = geocoder.arcgis('Bucharest, Romania', maxRows=10)

for code in codes:
    pprint((code.address, code.latlng))