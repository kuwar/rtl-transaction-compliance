from elasticsearch import Elasticsearch

es = Elasticsearch("http://localhost:9200")

if __name__ == "__main__":
    print(es.info().body)

