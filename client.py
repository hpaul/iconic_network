"""A Python module that provides the API client component for the elsapy package.
    Additional resources:
    * https://github.com/ElsevierDev/elsapy
    * https://dev.elsevier.com
    * https://api.elsevier.com"""

import requests, json, time, urllib
import log_util
from abc import ABCMeta, abstractmethod

logger = log_util.get_logger(__name__)


class ElsClient:
    """A class that implements a Python interface to api.elsevier.com"""

    # class variables
    __url_base = "https://api.elsevier.com/"    ## Base URL for later use
    __user_agent = "elsapy-v0.4.6"			        ## Helps track library use
    __min_req_interval = 0.23                    ## Min. request interval in sec
    __ts_last_req = time.time()                 ## Tracker for throttling

    # constructors
    def __init__(self, api_key, inst_token = None, num_res = 25):
        """Initializes a client with a given API Key and, optionally, institutional
            token, number of results per request, and local data path."""
        self.api_key = api_key
        self.inst_token = inst_token
        self.num_res = num_res

    # properties
    @property
    def api_key(self):
        """Get the apiKey for the client instance"""
        return self._api_key
    @api_key.setter
    def api_key(self, api_key):
        """Set the apiKey for the client instance"""
        self._api_key = api_key

    @property
    def inst_token(self):
        """Get the instToken for the client instance"""
        return self._inst_token
    @inst_token.setter
    def inst_token(self, inst_token):
        """Set the instToken for the client instance"""
        self._inst_token = inst_token

    @property
    def num_res(self):
        """Gets the max. number of results to be used by the client instance"""
        return self._num_res

    @num_res.setter
    def num_res(self, numRes):
        """Sets the max. number of results to be used by the client instance"""
        self._num_res = numRes

    @property
    def req_status(self):
    	'''Return the status of the request response, '''
    	return {'status_code': self._status_code, 'status_msg': self._status_msg}

    # access functions
    def getBaseURL(self):
        """Returns the ELSAPI base URL currently configured for the client"""
        return self.__url_base

    # request/response execution functions
    def exec_request(self, URL):
        """Sends the actual request; returns response."""

        ## Throttle request, if need be
        interval = time.time() - self.__ts_last_req
        if (interval < self.__min_req_interval):
            time.sleep( self.__min_req_interval - interval )

        ## Construct and execute request
        headers = {
            "X-ELS-APIKey"  : self.api_key,
            "User-Agent"    : self.__user_agent,
            "Accept"        : 'application/json'
            }
        if self.inst_token:
            headers["X-ELS-Insttoken"] = self.inst_token
        logger.info('Sending GET request to ' + URL)
        r = requests.get(
            URL,
            headers = headers
            )
        self.__ts_last_req = time.time()
        self._status_code=r.status_code
        if r.status_code == 200:
            self._status_msg='data retrieved'
            return json.loads(r.text)
        else:
            self._status_msg="HTTP " + str(r.status_code) + " Error from " + URL + " and using headers " + str(headers) + ": " + r.text
            raise requests.HTTPError("HTTP " + str(r.status_code) + " Error from " + URL + "\nand using headers " + str(headers) + ":\n" + r.text)


class ElsSearch:
    """Represents a search to one of the search indexes accessible
         through api.elsevier.com. Returns True if successful; else, False."""

    def __init__(self, URL):
        """Initializes a search object with a query and target index."""
        self._uri = URL

    @property
    def index(self):
        """Gets the label of the index targeted by the search"""
        return self._index

    @index.setter
    def index(self, index):
        self._index = index
        """Sets the label of the index targeted by the search"""

    @property
    def results(self):
        """Gets the results for the search"""
        return self._results

    @property
    def links(self):
        """Links for search"""
        return self._links

    @property
    def tot_num_res(self):
        """Gets the total number of results that exist in the index for
            this query. This number might be larger than can be retrieved
            and stored in a single ElsSearch object (i.e. 5,000)."""
        return self._tot_num_res

    @property
    def num_res(self):
        """Gets the number of results for this query that are stored in the
            search object. This number might be smaller than the number of
            results that exist in the index for the query."""
        return len(self.results)

    @property
    def cursor(self):
        """Get cursor identification for next page of scopus search"""
        return self._cursor

    @property
    def uri(self):
        """Gets the request uri for the search"""
        return self._uri

    def execute(self, els_client = None, get_all = False, limit = 1000):
        """Executes the search. If get_all = False (default), this retrieves
            the default number of results specified for the API. If
            get_all = True, multiple API calls will be made to iteratively get
            all results for the search, up to a maximum of 5,000."""
        ## TODO: add exception handling
        api_response = els_client.exec_request(self._uri)
        self._tot_num_res = int(api_response['search-results']['opensearch:totalResults'])
        if api_response['search-results'].get('cursor'):
            self._cursor = api_response['search-results']['cursor']
        if api_response['search-results'].get('entry'):
            self._results = api_response['search-results']['entry']
        else:
            self._results = []
        self._links = api_response['search-results']['link']
        if get_all is True:
            while (self.num_res < self.tot_num_res) and (self.num_res < limit):
                for e in api_response['search-results']['link']:
                    if e['@ref'] == 'next':
                        next_url = e['@href']
                api_response = els_client.exec_request(next_url)
                self._results += api_response['search-results']['entry']

    def hasAllResults(self):
        """Returns true if the search object has retrieved all results for the
            query from the index (i.e. num_res equals tot_num_res)."""
        return (self.num_res is self.tot_num_res)


class ElsEntity(metaclass=ABCMeta):
    """An abstract class representing an entity in Elsevier's data model"""

    # constructors
    @abstractmethod
    def __init__(self, uri):
        """Initializes a data entity with its URI"""
        self._uri = uri
        self._data = None
        self._client = None

    # properties
    @property
    def uri(self):
        """Get the URI of the entity instance"""
        return self._uri

    @uri.setter
    def uri(self, uri):
        """Set the URI of the entity instance"""
        self._uri = uri

    @property
    def id(self):
        """Get the dc:identifier of the entity instance"""
        return self.data["coredata"]["dc:identifier"]

    @property
    def int_id(self):
        """Get the (non-URI, numbers only) ID of the entity instance"""
        dc_id = self.data["coredata"]["dc:identifier"]
        return dc_id[dc_id.find(':') + 1:]

    @property
    def data(self):
        """Get the full JSON data for the entity instance"""
        return self._data

    @property
    def client(self):
        """Get the elsClient instance currently used by this entity instance"""
        return self._client

    @client.setter
    def client(self, elsClient):
        """Set the elsClient instance to be used by thisentity instance"""
        self._client = elsClient

    # modifier functions
    @abstractmethod
    def read(self, payloadType, elsClient):
        """Fetches the latest data for this entity from api.elsevier.com.
            Returns True if successful; else, False."""
        if elsClient:
            self._client = elsClient;
        elif not self.client:
            raise ValueError(
                '''Entity object not currently bound to elsClient instance. Call .read() with elsClient argument or set .client attribute.''')
        try:
            api_response = self.client.exec_request(self.uri)
            if isinstance(api_response[payloadType], list):
                self._data = api_response[payloadType][0]
            else:
                self._data = api_response[payloadType]
            ## TODO: check if URI is the same, if necessary update and log warning.
            logger.info("Data loaded for " + self.uri)
            return True
        except (requests.HTTPError, requests.RequestException) as e:
            for elm in e.args:
                logger.warning(elm)
            return False

    def write(self):
        """If data exists for the entity, writes it to disk as a .JSON file with
             the url-encoded URI as the filename and returns True. Else, returns
             False."""
        if (self.data):
            dataPath = self.client.local_dir / (urllib.parse.quote_plus(self.uri) + '.json')
            with dataPath.open(mode='w') as dump_file:
                json.dump(self.data, dump_file)
                dump_file.close()
            logger.info('Wrote ' + self.uri + ' to file')
            return True
        else:
            logger.warning('No data to write for ' + self.uri)
            return False


class AbsDoc(ElsEntity):
    """A document in Scopus. Initialize with URI or Scopus ID."""

    # static variables
    __payload_type = u'abstracts-retrieval-response'
    __uri_base = u'https://api.elsevier.com/content/abstract/'

    @property
    def title(self):
        """Gets the document's title"""
        return self.data["coredata"]["dc:title"];

    @property
    def uri(self):
        """Gets the document's uri"""
        return self._uri

    # constructors
    def __init__(self, uri = '', scp_id = ''):
        """Initializes a document given a Scopus document URI or Scopus ID."""
        if uri and not scp_id:
            super().__init__(uri)
        elif scp_id and not uri:
            super().__init__(self.__uri_base + 'scopus_id/' + str(scp_id))
        elif not uri and not scp_id:
            raise ValueError('No URI or Scopus ID specified')
        else:
            raise ValueError('Both URI and Scopus ID specified; just need one.')

    # modifier functions
    def read(self, els_client = None):
        """Reads the JSON representation of the document from ELSAPI.
             Returns True if successful; else, False."""
        if super().read(self.__payload_type, els_client):
            return True
        else:
            return False


class ElsProfile(ElsEntity, metaclass=ABCMeta):
    """An abstract class representing an author or affiliation profile in
        Elsevier's data model"""

    def __init__(self, uri):
        """Initializes a data entity with its URI"""
        super().__init__(uri)
        self._doc_list = None

    @property
    def doc_list(self):
        """Get the list of documents for this entity"""
        return self._doc_list

    @abstractmethod
    def read_docs(self, payloadType, els_client=None):
        """Fetches the list of documents associated with this entity from
            api.elsevier.com. If need be, splits the requests in batches to
            retrieve them all. Returns True if successful; else, False.
			NOTE: this method requires elevated API permissions.
			See http://bit.ly/2leirnq for more info."""
        if els_client:
            self._client = els_client;
        elif not self.client:
            raise ValueError(
                '''Entity object not currently bound to els_client instance. Call .read() with els_client argument or set .client attribute.''')
        try:
            api_response = self.client.exec_request(self.uri + "?view=documents")
            if isinstance(api_response[payloadType], list):
                data = api_response[payloadType][0]
            else:
                data = api_response[payloadType]
            docCount = int(data["documents"]["@total"])
            self._doc_list = [x for x in data["documents"]["abstract-document"]]
            for i in range(0, docCount // self.client.num_res):
                try:
                    api_response = self.client.exec_request(
                        self.uri + "?view=documents&start=" + str((i + 1) * self.client.num_res + 1))
                    if isinstance(api_response[payloadType], list):
                        data = api_response[payloadType][0]
                    else:
                        data = api_response[payloadType]
                    self._doc_list = self._doc_list + [x for x in data["documents"]["abstract-document"]]
                except  (requests.HTTPError, requests.RequestException) as e:
                    if hasattr(self, 'doc_list'):  ## We don't want incomplete doc lists
                        self._doc_list = None
                    raise e
            logger.info("Documents loaded for " + self.uri)
            return True
        except (requests.HTTPError, requests.RequestException) as e:
            logger.warning(e.args)
            return False

    def write_docs(self):
        """If a doclist exists for the entity, writes it to disk as a .JSON file
             with the url-encoded URI as the filename and returns True. Else,
             returns False."""
        if self.doc_list:
            dataPath = self.client.local_dir
            dump_file = open('data/'
                             + urllib.parse.quote_plus(self.uri + '?view=documents')
                             + '.json', mode='w'
                             )
            dump_file.write('[' + json.dumps(self.doc_list[0]))
            for i in range(1, len(self.doc_list)):
                dump_file.write(',' + json.dumps(self.doc_list[i]))
            dump_file.write(']')
            dump_file.close()
            logger.info('Wrote ' + self.uri + '?view=documents to file')
            return True
        else:
            logger.warning('No doclist to write for ' + self.uri)
            return False


class ElsAuthor(ElsProfile):
    """An author of a document in Scopus. Initialize with URI or author ID."""

    # static variables
    __payload_type = u'author-retrieval-response'
    __uri_base = u'https://api.elsevier.com/content/author/author_id/'

    # constructors
    def __init__(self, uri='', author_id=''):
        """Initializes an author given a Scopus author URI or author ID"""
        if uri and not author_id:
            super().__init__(uri)
        elif author_id and not uri:
            super().__init__(self.__uri_base + str(author_id))
        elif not uri and not author_id:
            raise ValueError('No URI or author ID specified')
        else:
            raise ValueError('Both URI and author ID specified; just need one.')

    # properties
    @property
    def first_name(self):
        """Gets the author's first name"""
        return self.data[u'author-profile'][u'preferred-name'][u'given-name']

    @property
    def last_name(self):
        """Gets the author's last name"""
        return self.data[u'author-profile'][u'preferred-name'][u'surname']

    @property
    def full_name(self):
        """Gets the author's full name"""
        return self.first_name + " " + self.last_name

        # modifier functions

    def read(self, els_client=None):
        """Reads the JSON representation of the author from ELSAPI.
            Returns True if successful; else, False."""
        if ElsProfile.read(self, self.__payload_type, els_client):
            return True
        else:
            return False

    def read_docs(self, els_client=None):
        """Fetches the list of documents associated with this author from
             api.elsevier.com. Returns True if successful; else, False."""
        return ElsProfile.read_docs(self, self.__payload_type, els_client)

    def read_metrics(self, els_client=None):
        """Reads the bibliographic metrics for this author from api.elsevier.com
             and updates self.data with them. Returns True if successful; else,
             False."""
        try:
            api_response = els_client.exec_request(
                self.uri + "?field=document-count,cited-by-count,citation-count,h-index,dc:identifier,coauthor-count&self-citation=exclude")
            data = api_response[self.__payload_type][0]
            if not self._data:
                self._data = dict()
                self._data['coredata'] = dict()
            if not data['coredata']:
                data['coredata'] = {}
            
            self._data['coredata']['dc:identifier'] = data['coredata']['dc:identifier']
            self._data['coredata']['citation-count'] = int(data['coredata']['citation-count'] or 0)
            self._data['coredata']['cited-by-count'] = int(data['coredata']['citation-count'] or 0)
            self._data['coredata']['document-count'] = int(data['coredata']['document-count'] or 0)
            self._data['h-index'] = int(data['h-index'])
            self._data['coauthor-count'] = int(data['coauthor-count'])
            logger.info('Added/updated author metrics')
        except (requests.HTTPError, requests.RequestException) as e:
            logger.warning(e.args)
            return False
        return True


class ElsAffil(ElsProfile):
    """An affilliation (i.e. an institution an author is affiliated with) in Scopus.
        Initialize with URI or affiliation ID."""

    # static variables
    __payload_type = u'affiliation-retrieval-response'
    __uri_base = u'https://api.elsevier.com/content/affiliation/affiliation_id/'

    # constructors
    def __init__(self, uri='', affil_id=''):
        """Initializes an affiliation given a Scopus affiliation URI or affiliation ID."""
        if uri and not affil_id:
            super().__init__(uri)
        elif affil_id and not uri:
            super().__init__(self.__uri_base + str(affil_id))
        elif not uri and not affil_id:
            raise ValueError('No URI or affiliation ID specified')
        else:
            raise ValueError('Both URI and affiliation ID specified; just need one.')

    # properties
    @property
    def name(self):
        """Gets the affiliation's name"""
        return self.data["affiliation-name"];

        # modifier functions

    def read(self, els_client=None):
        """Reads the JSON representation of the affiliation from ELSAPI.
             Returns True if successful; else, False."""
        if ElsProfile.read(self, self.__payload_type, els_client):
            return True
        else:
            return False

    def read_docs(self, els_client=None):
        """Fetches the list of documents associated with this affiliation from
              api.elsevier.com. Returns True if successful; else, False."""
        return ElsProfile.read_docs(self, self.__payload_type, els_client)