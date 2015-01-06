
import requests

class RestClient(object):

    def __init__(self, host, port, username, password):
        self.host = host
        self.port = port
        self.username = username
        self.password = password

        self.nodes = None
        self.buckets = None

    def update(self):
        self._get_nodes()
        self._get_buckets

    def get_nodes(self):
        if self.nodes is None:
            self._get_nodes()
        return self.nodes

    def get_bucket(self, bucket):
        if self.buckets is None:
            self._get_buckets()
        return self.buckets[bucket]

    def _get_nodes(self):
        nodes = dict()
        data = self._request('pools/default')
        for node in data['nodes']:
            name = node['hostname'].encode('ascii')
            nodes[name] = dict()
            nodes[name]['host'] = name.split(':')[0]
            nodes[name]['rest_port'] = int(name.split(':')[1])
            nodes[name]['data_port'] = node['ports']['direct']
            nodes[name]['proxy_port'] = node['ports']['proxy']
        self.nodes = nodes

    def _get_buckets(self):
        buckets = dict()
        data = self._request('pools/default/buckets')
        for bucket in data:
            name = bucket['name']
            buckets[name] = dict()
            buckets[name]['password'] = bucket['saslPassword']

            vbid = 0
            map = dict()
            nodes = bucket['vBucketServerMap']['serverList']
            vbmap = bucket['vBucketServerMap']['vBucketMap']
            for vbucket in vbmap:
                map[vbid] = nodes[vbucket[0]].encode('ascii')
                vbid += 1
            buckets[name]['vbmap'] = map

        self.buckets = buckets

    def _request(self, api):
        url = 'http://%s:%d/%s' % (self.host, self.port, api)
        r = requests.get(url, auth=(self.username, self.password))
        r.raise_for_status()
        return r.json()

