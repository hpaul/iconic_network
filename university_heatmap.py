from models import *
import os
import geocoder
import pandas as pd
import plotly as py
from pandas.io.json import loads
import plotly.graph_objs as go
import pickle
py.tools.set_credentials_file(username='ampero', api_key='NBtBgrbe6QM7KifHWufM')
py.offline.init_notebook_mode()

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
    unis_locations = load_obj('universities')

    if len(unis_locations) < 1:
        unis = data['university'].unique()
        unis_locations = dict()
        for uni in unis:
            geocode = geocoder.arcgis(uni, key='hpaul')
            unis_locations[uni] = [geocode.lat, geocode.lng]
        save_obj(unis_locations, 'universities')

    cities_locations = load_obj('cities')

    if len(cities_locations) < 1:
        cities = data['city'].unique()
        cities_locations = dict() 
        for city in cities:
            geocode = geocoder.arcgis(city, key='hpaul')
            cities_locations[city] = [geocode.lat, geocode.lng]

        save_obj(cities_locations, 'cities')
    
    return unis_locations,cities_locations


mapbox_access_token = 'pk.eyJ1IjoiYW1wZXJvIiwiYSI6ImNqcXNjZjVicDB3amE0MmxweTducnpia3YifQ.S3pko1wTKcqdBUt0y5wWMg'

authors = Coauthors \
    .select(Coauthors.id, Author.full_name, Author.cited_by_count, Author.affiliation_current) \
    .join(Author, on=(Coauthors.id == Author.id)) \
    .where(Coauthors.saved == 1) \
    .where(Author.cat.contains('PHYS')) \
    .order_by(Author.cited_by_count.desc()) \
    .limit(1200)

sql = authors.sql()
# Fix surplus quotes
sql[1][1] = '%PHYS%'
df = pd.read_sql(sql[0], db.connection(), params=sql[1])

def map_city(row):
    return "{}, {}".format(row.get('affiliation-city'), row.get('affiliation-country'))

df['affiliation_current'] = df['affiliation_current'].map(loads)
df['university'] = df['affiliation_current'].map(lambda row: row.get('affiliation-name'))
df['city'] = df['affiliation_current'].map(map_city)
df['name'] = df['full_name'].map(loads)
df['name'] = df['name'].map(lambda row: row.get('initials'))
df.drop(['affiliation_current'],axis=1, inplace=True)

unis, cities = get_offline_data(df)

def get_lat(row):
    uni = unis[row['university']]
    city = cities[row['city']]
    if uni[0] is None:
        return city[0]
    else:
        return uni[0]

def get_long(row):
    uni = unis[row['university']]
    city = cities[row['city']]
    if uni[1] is None:
        return city[1]
    else:
        return uni[1]

# Add latitude and longitude for every author
df['lat'] = df[['university', 'city']].apply(get_lat, axis=1)
df['long'] = df[['university', 'city']].apply(get_long, axis=1)

scl = [ [0,"rgb(5, 10, 172)"],[0.35,"rgb(40, 60, 190)"],[0.5,"rgb(70, 100, 245)"],\
    [0.6,"rgb(90, 120, 245)"],[0.7,"rgb(106, 137, 247)"],[1,"rgb(220, 220, 220)"] ]

data = [
    go.Scattermapbox(
        lat = df['lat'],
        lon = df['long'],
        text = df['name'],
        #customdata=df[['name', 'university', 'city']],
        #hovertext=df['city'],
        mode = 'markers+text',
        marker = dict(
            size = 14,
            opacity = 0.2,
            reversescale = True,
            autocolorscale = False,
            symbol = 'circle',
            # line = dict(
            #     width=1,
            #     color='rgba(102, 102, 102)'
            # ),
            colorscale = scl,
            cmin = 0,
            color = df['cited_by_count'],
            cmax = df['cited_by_count'].max(),
            colorbar=dict(
                title="Number of citations"
            )
        ))
]

layout = dict(
        title = 'Heatmap of best cited authors from Physics',
        showlegend = True,
        autosize=True,
        hovermode='closest',
        mapbox=dict(
            accesstoken=mapbox_access_token,
            style='light',
            bearing=0,
            pitch=0,
            zoom=1
        )
        # geo = dict(
        #     scope='europe',
        #     showland = True,
        #     showcoastlines=True,
        #     showocean=False,
        #     landcolor = 'rgb(217, 217, 217)',
        #     showsubunits=True,
        #     subunitwidth=1,
        #     countrywidth=1,
        #     #subunitcolor="rgb(255, 255, 255)",
        #     countrycolor="rgb(255, 255, 255)"
        # ),
    )

fig = dict(data=data, layout=layout)
py.plotly.iplot(fig, validate=True, show_link=True, image='png', filename='authors_heatmap', auto_open=True)
