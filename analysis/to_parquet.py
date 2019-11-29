import findspark
findspark.init()

import os
from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import pyspark.sql.functions as F
import yaml
# load spark stuff
conf = SparkConf()
# Add sqlite driver
conf.set('spark.driver.extraClassPath', os.path.abspath('./sqlite-jdbc.jar'))
conf.set('spark.executor.extraClassPath', os.path.abspath('./sqlite-jdbc.jar'))
conf.set('spark.sql.parquet.compression.codec', 'gzip')
sc = SparkContext.getOrCreate(conf=conf)
sqlContext = SQLContext(sc)

# List of official countries names
with open('../countries.yaml', 'r') as f:
    countries = list((yaml.load(f))['countries'].values())
    countries.extend(['Moldova','South Korea'])

def get_country_name(data):
    if data:
        computed = ''
        for name in data.split(' '):
            computed = ("{} {}".format(computed, name)).strip()
            if computed in countries:
                return computed
            else:
                continue

    return data


def save_authors():
    connectionProperties = { 
        #"customSchema": 'id INT, co_list STRING, last_page INT, saved INT',
        "customSchema": 'id LONG, full_name STRING, subject_areas STRING, document_count INT,cited_by_count INT,citations_count INT,h_index INT,coauthors_count INT,affiliation_current STRING,cat STRING,country STRING,docs_fetched INT,last_page INT,is_sample INT,citations INT',
        "driver": 'org.sqlite.JDBC'
    }

    df = sqlContext.read.jdbc(url='jdbc:sqlite:../iconic.db', table='author', properties=connectionProperties)
    full_name = F.concat_ws(' ', F.get_json_object(df.full_name, '$[*].surname'), F.get_json_object(df.full_name, '$[*].given-name'))
    country = F.get_json_object(df.affiliation_current, '$.affiliation-country')
    city = F.get_json_object(df.affiliation_current, '$.affiliation-city')
    university = F.get_json_object(df.affiliation_current, '$.affiliation-name')
    
    country_name = F.udf(lambda c: get_country_name(c), StringType())

    df = df.withColumn('full_name', full_name)
    df = df.withColumn('country', country_name(country))
    df = df.withColumn('city',city)
    df = df.withColumn('university',university)
    df = df.drop('document_count', 'h_index', 'citations_count', 'coauthors_count', 'affiliation_current', 'docs_fetched', 'last_page', 'is_sample','citations')

    df.write.mode('overwrite').parquet(os.path.abspath('./data/authors'))


def save_collaborations():
    auths_struct = StructType([StructField("list", ArrayType(StringType()))])

    connectionProperties = { 
        "customSchema": 'abs_id LONG, authors STRING, published STRING, cited_by INT, keywords STRING, coll_count INT, h_index INT, message STRING, saved INT',
        "driver": 'org.sqlite.JDBC'
    }

    df = sqlContext.read.jdbc(url='jdbc:sqlite:../iconic.db', table='collaboration', properties=connectionProperties)
    
    df = df.withColumn('authors', F.get_json_object(df.authors, '$[*].authid'))

    map_auths = F.udf(lambda s: '{{"list": {}}}'.format(s), StringType())
    df = df.withColumn('auths', map_auths(df.authors))
    df = df.withColumn('authors', F.from_json(df.auths, auths_struct))
    df = df.withColumn("authors_size", F.size(F.col('authors.list')))
    new_df = df.select('abs_id', 'published', 'cited_by', 'keywords', F.explode(df.authors.list).alias('author_id'))

    new_df.write.parquet(os.path.abspath('./data/collaborations'))


def save_coauthors():
    auths_struct = StructType([StructField("list", ArrayType(StringType()))])

    connectionProperties = { 
        "customSchema": 'id LONG, co_list STRING, last_page INT, saved INT',
        "driver": 'org.sqlite.JDBC'
    }

    df = sqlContext.read.jdbc(url='jdbc:sqlite:../iconic.db', table='coauthors', properties=connectionProperties)
    
    map_auths = F.udf(lambda s: '{{"list": {}}}'.format(s), StringType())
    df = df.withColumn('auths', map_auths(df.co_list))
    df = df.withColumn('co_list', F.from_json(df.auths, auths_struct))
    df = df.withColumn("authors_size", F.size(F.col('co_list.list')))
    new_df = df.select(df.id.alias('network_id'), F.explode(df.co_list.list).alias('author_id'))

    new_df.write.mode('overwrite').parquet(os.path.abspath('./data/coauthors'))


# save_authors()
save_collaborations()
# save_coauthors()