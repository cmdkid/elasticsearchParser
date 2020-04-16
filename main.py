import os

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

'''
elasticsearch.url
          value: http://elasticsearch:9200
        - name: elasticsearch.username
          value: "elastic"
        - name: elasticsearch.password
'''

var_list = ['elasticsearch.url', 'elasticsearch.username', 'elasticsearch.password']
if not all(item in os.environ.keys() for item in var_list):  # os.environ.get('HOME', '/home/username/')
    raise ValueError('vars: elasticsearch.url, elasticsearch.username, elasticsearch.password not set!')
else:
    e_url = os.environ['elasticsearch.url']
    e_login = os.environ['elasticsearch.username']
    e_pass = os.environ['elasticsearch.password']

cur_index = 'logstash-2020.04.16'
client = Elasticsearch(e_url)

s = Search(using=client, index=cur_index)
'''
    .filter("term", category="search") \
    .query("match", title="python")   \
    .exclude("match", description="beta")
'''

s.aggs.bucket('per_tag', 'terms', field='tags') \
    .metric('max_lines', 'max', field='lines')

response = s.execute()

for hit in response:
    print(hit.meta.score, hit.title)

for tag in response.aggregations.per_tag.buckets:
    print(tag.key, tag.max_lines.value)
