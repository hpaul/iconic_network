import folium
import pandas as pd
import geocoder
import os
import pickle

def save_obj(obj, name):
    with open('obj/'+ name + '.pkl', 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)

def load_obj(name):
    if os.path.exists('obj/' + name + '.pkl'):
        with open('obj/' + name + '.pkl', 'rb') as f:
            return pickle.load(f)
    else:
        return []

def get_offline_data(data):
    countries_loc = load_obj('countries')

    if len(countries_loc) < 1:
        countries = data['Name'].unique()
        countries_loc = []
        for c in countries:
            geocode = geocoder.arcgis(c, key='hpaul')
            countries_loc.append({
                'country': c,
                'lat': geocode.lat,
                'lng': geocode.lng
            })
        
        save_obj(countries_loc, 'countries')

    return countries_loc


nodes = pd.read_csv('./country_data/nodes.csv')
nodes_2017 = nodes[nodes['Year'] == 2017]

countries = pd.DataFrame(get_offline_data(nodes))
countries.to_json('./country_data/countries.json', orient='records')


