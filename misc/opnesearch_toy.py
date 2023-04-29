from opensearchpy import OpenSearch
import json
host = 'localhost'
port = 9200
auth = ('admin', 'admin') # For testing only. Don't store credentials in code.

# Create the client with SSL/TLS enabled, but hostname and certification verification disabled.
client = OpenSearch(
    hosts = [{'host': host, 'port': port}],
    http_compress = True, # enables gzip compression for request bodies
    http_auth = auth,
    # client_cert = client_cert_path,
    # client_key = client_key_path,
    use_ssl = True,
    verify_certs = False,
    ssl_assert_hostname = False,
    ssl_show_warn = False,
)

# Do a few checks before we start indexing:
print(client.cat.health())
print(client.cat.indices())

# If you still have your documents from the Dev Tools test, we should be able to check them here:
print(client.cat.count("search_fun_test", params={"v":"true"}))



# Create an index with non-default settings.
index_name = 'search_fun_revisited'
index_body = {
  'settings': {
    'index': {
      'query':{
          'default_field': "body"
      }
    }
  }
}

response = client.indices.create(index_name, body=index_body)
print('\nCreating index:')
print(response)


# Add our sample document to the index.
docs = [
    {
        "id": "doc_a",
        "title": "Fox and Hounds",
        "body": "The quick red fox jumped over the lazy brown dogs.",
        "price": "5.99",
        "in_stock": True,
        "category": "childrens"},
    {
        "id": "doc_b",
        "title": "Fox wins championship",
        "body": "Wearing all red, the Fox jumped out to a lead in the race over the Dog.",
        "price": "15.13",
        "in_stock": True,
        "category": "sports"},
    {
        "id": "doc_c",
        "title": "Lead Paint Removal",
        "body": "All lead must be removed from the brown and red paint.",
        "price": "150.21",
        "in_stock": False,
        "category": "instructional"},
    {
        "id": "doc_d",
        "title": "The Three Little Pigs Revisted",
        "price": "3.51",
        "in_stock": True,
        "body": "The big, bad wolf huffed and puffed and blew the house down. The end.",
        "category": "childrens"}
]

for doc in docs:
    doc_id = doc["id"]
    print("Indexing {}".format(doc_id))
    response = client.index(
        index=index_name,
        body=doc,
        id=doc_id,
        refresh=True
    )
    print('\n\tResponse:')
    print(response)

# Verify they are in:
print(client.cat.count(index_name, params={"v": "true"}))

from opensearchpy.helpers import bulk

index_name = 'search_fun_bulk'
index_body = {
    'settings': {
        'index': {
            'query': {
                'default_field': "body"
            }
        }
    }
}

client.indices.create(index_name, body=index_body)

# Add our sample document to the index.
docs = [
    {
        "id": "doc_a",
        '_index': index_name,
        "title": "Fox and Hounds",
        "body": "The quick red fox jumped over the lazy brown dogs.",
        "price": "5.99",
        "in_stock": True,
        "category": "childrens"},
    {
        "id": "doc_b",
        '_index': index_name,
        "title": "Fox wins championship",
        "body": "Wearing all red, the Fox jumped out to a lead in the race over the Dog.",
        "price": "15.13",
        "in_stock": True,
        "category": "sports"},
    {
        "id": "doc_c",
        '_index': index_name,
        "title": "Lead Paint Removal",
        "body": "All lead must be removed from the brown and red paint.",
        "price": "150.21",
        "in_stock": False,
        "category": "instructional"},
    {
        "id": "doc_d",
        '_index': index_name,
        "title": "The Three Little Pigs Revisted",
        "price": "3.51",
        "in_stock": True,
        "body": "The big, bad wolf huffed and puffed and blew the house down. The end.",
        "category": "childrens"}
]

bulk(client, docs)

print(client.cat.count(index_name, params={"v": "true"}))

print(client.indices.get_mapping(index_name))

index_name = 'search_fun_revisited_custom_mappings'
index_body = {
    'settings': {
        'index': {
            'query': {
                'default_field': "body"
            }
        }
    },
    "mappings": {
        "properties": {
            "title": {"type": "text", "analyzer": "english"},
            "body": {"type": "text", "analyzer": "english"},
            "in_stock": {"type": "boolean"},
            "category": {"type": "keyword", "ignore_above": "256"},
            "price": {"type": "float"}
        }
    }
}

client.indices.create(index_name, body=index_body)

for doc in docs:
    doc_id = doc["id"]
    print("Indexing {}".format(doc_id))
    response = client.index(
        index=index_name,
        body=doc,
        id=doc_id,
        refresh=True
    )
    print('\n\tResponse:')
    print(response)


q = 'dogs'
index_name = 'search_fun_revisited_custom_mappings'
query = {
  'size': 5,
  'query': {
    'multi_match': {
      'query': q,
      'fields': ['title^2', 'body']
    }
  }
}

import pprint

pprint.pprint(client.search(
    body = query,
    index = index_name
))

# try a match all query with a filter and a price factor
query = {
    'size': 5,
    'query': {
        "function_score": {
            "query": {
                "bool": {
                    "must": [
                        {"match_all": {}}
                    ],
                    "filter": [
                        {"term": {"category": "childrens"}}
                    ]
                }
            },
            "field_value_factor": {
                "field": "price",
                "missing": 1
            }
        }
    }
}

pprint.pprint(client.search(
    body=query,
    index=index_name
))


query = {
    'size': 0,
    'query': {
        "match_all": {}
    },
    'aggs': {
        "category": {
            "terms": {
                "field": "category",
                "size": 10,
                "missing": "N/A",
                "min_doc_count": 0
            }
        }
    }
}

response = client.search(
    body=query,
    index=index_name
)
print('\nSearch results:')
print(json.dumps(response, indent=4))

query = {
    'size': 0,
    'query': {
        "match_all": {}
    },
    'aggs': {
        "price": {
            "range": {
                "field": "price",
                "ranges": [
                    {
                        "to": 5
                    },
                    {
                        "from": 5,
                        "to": 20
                    },
                    {
                        "from": 20,
                    }
                ]
            }
        }
    }
}

response = client.search(
body = query,
index = index_name
)
print('\nSearch results:')
print(json.dumps(response, indent=4))
