from elasticsearch import Elasticsearch
import simplejson as json
import os
import xml.etree.ElementTree as ET


es = Elasticsearch(['http://elastic:changeme@localhost:9200/'])


def add_articles():
    for idx, article in enumerate(os.listdir('../ousd-articles')):
        print(str(idx) + " " + article)
        root = ET.parse('../ousd-articles/' + article)

        index_article(root.findall('.//text')[0].text,root.findall('.//title')[0].text, root.findall('.//abstract')[0].text, root.findall('.//tags')[0], os.path.splitext(os.path.basename(article))[0])

def create_index():
    client.indices.create(
        index=ousd,
        body={
            'settings': {
                # just one shard, no replicas for testing
                'number_of_shards': 1,
                'number_of_replicas': 0,

                # custom analyzer for analyzing file paths
                'analysis': {
                    'analyzer': {
                        'file_path': {
                            'type': 'custom',
                            'tokenizer': 'path_hierarchy',
                            'filter': ['lowercase']
                        }
                    }
                }
            }
        },
        # Will ignore 400 errors, remove to ensure you're prompted
        ignore=400
    )

def index_article(body_text, title, abstract, tags, id):
    if body_text is not None and title is not None and abstract is not None :
        es_body = {"text":body_text.strip(), "title":title.strip(), "tags": [tag.text for tag in tags.findall('.//tag')], "abstract":abstract}
        es.index(index='ousd', doc_type='article', id=id, body=es_body)

def search(query, numResults):
    results = es.search(index="ousd", q=query, size=numResults)
    return results

def save_results(results):
    for article in results['hits']['hits']:
        with open("artificial_intelligence/"+article.get('_id') + '.json', 'w') as f:
            f.write(json.dumps(article.get('_source')))

if __name__ == '__main__':
    #add_articles()
    #create_index()
    results = search("artificial intelligence", 200)
    save_results(results)