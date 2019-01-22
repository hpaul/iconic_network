from requests import Request, exceptions
from client import ElsClient, ElsSearch, ElsAuthor
from models import *
import pprint as pp

# Load configuration
con_file = open("config.json")
config = json.load(con_file)
con_file.close()

# Initialize elsclient client
client = ElsClient(config['apikey'])
arr = [57196615101, 7006512072, 7003846093, 55490207700, 6701529045, 6603230677, 25723888600, 6602751450, 6601950682, 8542734300, 7406560221, 23009268700, 7007012322, 7003953125, 56416261100, 57197166575, 7006085625, 56040648700] 

for a_id in arr:
    metrics = ElsAuthor(author_id=a_id)
    
    if metrics.read_metrics(client):
        document_count = str(metrics.data['coredata'].get('document-count') or 0)
        cited_by_count = str(metrics.data['coredata'].get('cited-by-count') or 0)

        print("UPDATE author SET cited_by_count='{}', document_count='{}' WHERE id='{}'".format(cited_by_count, document_count, str(a_id)))