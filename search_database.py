
import pymysql
from elasticsearch import Elasticsearch

# Connect to Elasticsearch
es = Elasticsearch("http://localhost:9200")

# Index name
INDEX_NAME = "local_texts"

# Define the index mapping with a suitable analyzer for text
index_mapping = {
    "mappings": {
        "properties": {
            "text": {
                "type": "text",
                "analyzer": "standard",  # Use a specific analyzer like "bengali_analyzer" if configured
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            }
        }
    }
}

# Ensure the Elasticsearch index exists
if not es.indices.exists(index=INDEX_NAME):
    es.indices.create(index=INDEX_NAME, body=index_mapping)
    print(f"Index '{INDEX_NAME}' created with text mapping and analyzer!")


def index_document(doc, doc_id):
    """
    Indexes a document in Elasticsearch.

    Parameters:
    - doc (dict): The document to index.
    - doc_id (int): The database ID, used as the Elasticsearch document ID.
    """
    es.index(index=INDEX_NAME, id=str(doc_id), body=doc)


def search_matching_documents(search_terms):
    """
    Searches for documents matching the specified terms.

    Parameters:
    - search_terms (list): List of terms to search for.

    Returns:
    - list: List of IDs of matching documents.
    """
    # Construct the search query
    should_clauses = [{"match": {"text": term}} for term in search_terms]

    # Perform the search
    response = es.search(index=INDEX_NAME, body={
        "query": {
            "bool": {
                "should": should_clauses,
                "minimum_should_match": 2  # At least one term should match
            }
        }
    })

    # Extract IDs of matching documents
    return [hit["_id"] for hit in response["hits"]["hits"]]


# Database connection
db_connection = pymysql.connect(
    host="localhost",
    user="root",
    password="",
    database="aibot_lic_update"
)

try:
    with db_connection.cursor(pymysql.cursors.SSCursor) as cursor:
        # Fetch rows from the `news` table one by one
        query = "SELECT id, details_content FROM news"
        cursor.execute(query)

        # Index all rows in Elasticsearch
        for row in cursor:
            news_id = row[0]
            content = row[1]

            # Prepare the document for Elasticsearch
            doc = {"text": content}

            # Index the document with its ID
            index_document(doc, news_id)

        print("All documents indexed successfully!")

        # Search terms to look for in Elasticsearch
        search_terms = ["ভারতে চ্যাম্পিয়ন","ফ্লাইটে"]

        # Find matching documents
        matching_ids = search_matching_documents(search_terms)

        print(len(matching_ids))
        print(f"Matching IDs: {matching_ids}" if matching_ids else "No matches found.")

except Exception as e:
    print(f"Error: {e}")

finally:
    db_connection.close()
