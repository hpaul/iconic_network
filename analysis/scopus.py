import requests
import time
import json

class NotFound(Exception):
    def __init__(self, message):
        # Call the base class constructor with the parameters it needs
        super().__init__(message,[])


class Metadata:
    # Metadata about scopus response
    total = 0
    per_page = 10
    index = 0
    links = []
    # Each facet belongs to a key which holds specified data
    facets = {}

    @property
    def total_pages(self):
        return int(round((self.total / self.per_page),0))
    @property
    def current_page(self):
        return int((self.total / self.per_page) - (self.total - (self.index+self.per_page) / self.per_page))

class ScopusResponse:
    # TODO: implement a universal message statuses
    status_message = ''
    metadata = Metadata()
    data = []

class ScopusAuthor:
    id = 0
    raw = ''
    name = ''
    documents = 0
    cited_by = 0
    country = ''
    city = ''
    affiliation_id = 0
    affiliation = ''
    subject_areas = []
    h_index = 0
    coauthors = []
    request = None

    def get_coauthors(self):
        params = {
            'co-author': self.id,
            'count': 25,
            'start': 0
        }
        url = "https://api.elsevier.com/content/search/author"
        req = requests.get(url, params=params, headers=self.request.headers)
        data = req.json()
        data = data['search-results']

        total = int(data['opensearch:totalResults'])
        per_page = int(data['opensearch:itemsPerPage'])

        for author in data['entry']:
            self.save_coauthor(author)

        for page in range(1, int(round((total / per_page),0))+1):
            params = {
                'co-author': self.id,
                'count': 25,
                'start': per_page*page
            }
            url = "https://api.elsevier.com/content/search/author"
            req = self.request.get(url, params=params)
            data = req.json()
            data = data['search-results']

            if data.get('entry') and isinstance(data.get('entry'), list):
                for author in data['entry']:
                    self.save_coauthor(author)

        return self.coauthors

    def save_coauthor(self, data):
        author = ScopusAuthor()
        
        if data.get('dc:identifier'):
            author.id = int((data.get('dc:identifier').split('AUTHOR_ID:'))[1])
        if data.get('document-count'):
            author.documents = int(data.get('document-count'))
        if data.get('cited-by-count'):
            author.cited_by = int(data.get('cited-by-count'))
        if data.get('h-index'):
            author.h_index = data.get('h-index')

        affiliation = data.get('affiliation-current')
        if affiliation:
            author.country = affiliation.get('affiliation-country')
            author.city = affiliation.get('affiliation-city')
            if affiliation.get('@id'):
                author.affiliation_id = int(affiliation.get('@id'))
            author.affiliation = affiliation.get('affiliation-name')
        
        if data.get('subject-area') and isinstance(data.get('subject-area'), list):
            author.subject_areas = [
                { 'name': sa.get('@abbrev'), 'freq': sa.get('@frequency')} 
                for sa in data.get('subject-area')
            ]
        # else:
        #     # TODO: Log none response
        #     raise NotFound('Author basic data could not be received!')


        self.coauthors.append(author)

class Scopus:
    # Track time between requests
    last_request_time = time.time()
    min_request_interval = 0
    response = ScopusResponse()
    search_cursor = None
    request = requests.Session()
    
    def __init__(self, api_key=None, min_request_interval=0.25):
        if not api_key:
            raise Exception("Set api for Scopus requests!")

        self.request.headers.update({
            'Accept': 'application/json',
            'X-ELS-APIKey': api_key ,
            'User-Agent': 'elsapi-v4.',
        })
        self.min_request_interval = min_request_interval

    def search(self, query, facets=[], sortby=None, date='', subject=None, per_page=100, start=None, fields=[]):
        ## Wait between requests, scopus API has a cooldown period
        interval = time.time() - self.last_request_time
        if (interval < self.min_request_interval):
            time.sleep(self.min_request_interval - interval)
        
        # Create search params specific to Scopus article retrival
        params = {
            'query': query,
            'subj': subject,
            'sort': sortby,
            'count': per_page,
            'start': start,
            'date': date,
            'field': ','.join(fields),
            'facets': ';'.join(facets)
        }
        # Filter for none params
        params = {k:v for k,v in params.items() if v is not None}
        url = "https://api.elsevier.com/content/search/scopus"
        req = self.request.get(url, params=params)
        self.last_request_time = time.time()
        
        # Check for successfully requests or throw error
        if req.status_code == 200:
            self.parse_response(req, type='scopus')
            return self
        else:
            raise requests.HTTPError(json.loads(req.text),req.headers)

    def search_authors(self, query, facets=[], sortby=None, date='', subject=None, per_page=100, start=None, fields=[]):
        ## Wait between requests, scopus API has a cooldown period
        interval = time.time() - self.last_request_time
        if (interval < self.min_request_interval):
            time.sleep(self.min_request_interval - interval)
        
        # Create search params specific to Scopus article retrival
        params = {
            'query': query,
            'subj': subject,
            'sort': sortby,
            'count': per_page,
            'start': start,
            'date': date,
            'field': ','.join(fields),
            'facets': ';'.join(facets)
        }
        # Filter for none params
        params = {k:v for k,v in params.items() if v is not None}
        url = "https://api.elsevier.com/content/search/author"
        req = self.request.get(url, params=params)
        self.last_request_time = time.time()
        
        # Check for successfully requests or throw error
        if req.status_code == 200:
            self.parse_response(req, type='scopus')
            return self
        else:
            raise requests.HTTPError(json.loads(req.text),req.headers)


    def author_detail(self, id, view='LIGHT', fields=[]):
        ## Wait between requests, scopus API has a cooldown period
        interval = time.time() - self.last_request_time
        if (interval < self.min_request_interval):
            time.sleep(self.min_request_interval - interval)

        # Create search params specific to Scopus article retrival
        params = {
            'view': view,
            'field': ','.join(fields)
        }
        # Filter for none params
        params = {k:v for k,v in params.items() if v is not None}
        url = "https://api.elsevier.com/content/author/author_id/{}".format(id)
        req = self.request.get(url, params=params)
        self.last_request_time = time.time()
        
        # Check for successfully requests or throw error
        if req.status_code == 200:
            self.parse_author(req)
            return self
        else:
            raise requests.HTTPError(json.loads(req.text),req.headers)

    def parse_response(self, req, type='search'):
        data = req.json()
        # TODO: Switch case per response type
        data = data['search-results']
        self.response.metadata.total = int(data['opensearch:totalResults'])
        self.response.metadata.per_page = int(data['opensearch:itemsPerPage'])
        self.response.metadata.index = int(data['opensearch:startIndex'])
        self.response.data = data['entry']
        
        facets = data.get('facet')
        if isinstance(facets,dict):
            self.response.metadata.facets[facets['name']] = facets['category']
        
        if isinstance(facets,list):
            for facet in facets:
                self.response.metadata.facets[facet['name']] = facet['category']

    def parse_author(self, req):
        data = req.json()
        r_type = '{}'.format(type)
        data = data['author-retrieval-response'][0]
        author = ScopusAuthor()
        author.raw = data
        # Use same instance to requst further data from scopus API
        author.request = self.request
        
        coredata = data.get('coredata')
        if coredata:
            author.id = int((coredata.get('dc:identifier').split('AUTHOR_ID:'))[1])
            author.name = ''
            author.documents = int(coredata.get('document-count'))
            author.cited_by = int(coredata.get('cited-by-count'))
            author.h_index = data.get('h-index')

            affiliation = data.get('affiliation-current')
            if affiliation:
                author.country = affiliation.get('affiliation-country')
                author.city = affiliation.get('affiliation-city')
                author.affiliation_id = affiliation.get('@id')
                author.affiliation = affiliation.get('affiliation-name')
                author.subject_areas = []
        else:
            raise NotFound('Author basic data could not be received!')
        
        self.response.data = author

    # def parse_facets(self, facets)

    # def search_cursor(query, facets='', limit=100, count=100):