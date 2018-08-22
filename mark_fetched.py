from models import *
import re

nohup = open('nohup.out', 'r', encoding='UTF-8')
data = nohup.read()
re_auth = re.compile('Getting documents for: "(.*)"')

fetched_authors = re_auth.findall(data)
fetched_authors = [int(a) for a in fetched_authors]

authors = Author.update(docs_fetched=False)
authors.execute()

