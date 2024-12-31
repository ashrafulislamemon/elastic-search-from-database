# import pymysql
# from elasticsearch import Elasticsearch, helpers

# # Connect to Elasticsearch
# es = Elasticsearch("http://localhost:9200")

# # Index name
# INDEX_NAME = "global_texts"

# # Define the index mapping with a suitable analyzer for text
# index_mapping = {
#     "mappings": {
#         "properties": {
#             "text": {
#                 "type": "text",
#                 "analyzer": "standard",  # Use "bengali_analyzer" if configured
#                 "fields": {
#                     "keyword": {
#                         "type": "keyword",
#                         "ignore_above": 256
#                     }
#                 }
#             }
#         }
#     }
# }

# # Ensure the Elasticsearch index exists
# if not es.indices.exists(index=INDEX_NAME):
#     es.indices.create(index=INDEX_NAME, body=index_mapping)
#     print(f"Index '{INDEX_NAME}' created with text mapping and analyzer!")


# def index_documents_bulk(news_data):
#     """
#     Index multiple documents in Elasticsearch using the bulk API.
#     """
#     actions = [
#         {
#             "_index": INDEX_NAME,
#             "_id": str(news['id']),
#             "_source": {"text": news['details_content']}
#         }
#         for news in news_data
#     ]

#     helpers.bulk(es, actions)
#     print(f"Indexed {len(actions)} documents successfully!")


# def fetch_news_data():
#     """
#     Fetch all news data from the MySQL database.
#     """
#     db_connection = pymysql.connect(
#         host="localhost",
#         user="root",
#         password="",
#         database="aibot_lic_update"
#     )

#     query = "SELECT id, details_content FROM news"
    
#     try:
#         with db_connection.cursor(pymysql.cursors.DictCursor) as cursor:
#             cursor.execute(query)
#             return cursor.fetchall()
#     except Exception as e:
#         print(f"Error fetching data from MySQL: {e}")
#         return []
#     finally:
#         db_connection.close()


# def search_matching_documents(search_terms):
#     """
#     Searches for documents matching the specified terms.
#     """
#     should_clauses = [{"match": {"text": term}} for term in search_terms]

#     response = es.search(index=INDEX_NAME, body={
#         "query": {
#             "bool": {
#                 "should": should_clauses,
#                 "minimum_should_match": 1  # At least one term should match
#             }
#         }
#     })

#     matching_ids = [hit["_id"] for hit in response["hits"]["hits"]]
#     print(f"Matching IDs: {matching_ids}")
#     return matching_ids


# def get_news_data_by_ids(ids):
#     """
#     Fetches the full news data from the MySQL database based on the given list of IDs.
#     """
#     if not ids:
#         return []

#     db_connection = pymysql.connect(
#         host="localhost",
#         user="root",
#         password="",
#         database="aibot_lic_update"
#     )

#     try:
#         with db_connection.cursor(pymysql.cursors.DictCursor) as cursor:
#             # Use tuple(ids) to format the query correctly
#             query = f"SELECT id, datetime, agency, title, details_link, details_content, sentiment, summary, category_id, type, status, created_at, updated_at FROM news WHERE id IN ({','.join(['%s'] * len(ids))})"
#             cursor.execute(query, tuple(ids))  # Pass the tuple here
#             return cursor.fetchall()
#     except Exception as e:
#         print(f"Error fetching news data: {e}")
#         return []
#     finally:
#         db_connection.close()


# def index_news_from_db():
#     """
#     Fetch and index news data from MySQL into Elasticsearch.
#     """
#     news_data = fetch_news_data()
#     if news_data:
#         index_documents_bulk(news_data)
#     else:
#         print("No news data found to index.")


# def get_matching_facebook_documents(search_terms):
#     """
#     Fetch matching documents from Elasticsearch and MySQL.
#     """
#     matching_ids = search_matching_documents(search_terms)

#     if matching_ids:
#         news_data = get_news_data_by_ids(matching_ids)
#         return {"matching_documents": news_data} if news_data else {"message": "No detailed news data found for the matching IDs."}
#     else:
#         return {"message": "No matches found in Elasticsearch."}


# # Index news from database (only needs to be done once or when data changes)
# index_news_from_db()

# # Example: Search for documents
# search_terms = ["তারেক"]  # Add your Bengali search terms
# matching_data = get_matching_facebook_documents(search_terms)

# print(matching_data)



# import pymysql
# from elasticsearch import Elasticsearch, helpers

# # Connect to Elasticsearch
# es = Elasticsearch("http://localhost:9200")

# # Index names
# INDEX_NAME_1 = "global_texts"
# INDEX_NAME_2 = "aibot_lic_update_news"

# # Define the index mapping for the two indices
# index_mapping_1 = {
#     "mappings": {
#         "properties": {
#             "text": {
#                 "type": "text",
#                 "analyzer": "standard",  # Use "bengali_analyzer" if configured
#                 "fields": {
#                     "keyword": {
#                         "type": "keyword",
#                         "ignore_above": 256
#                     }
#                 }
#             }
#         }
#     }
# }

# index_mapping_2 = {
#     "mappings": {
#         "properties": {
#             "text": {
#                 "type": "text",
#                 "analyzer": "standard",  # Use "bengali_analyzer" if configured
#                 "fields": {
#                     "keyword": {
#                         "type": "keyword",
#                         "ignore_above": 256
#                     }
#                 }
#             }
#         }
#     }
# }

# # Ensure the Elasticsearch indices exist
# if not es.indices.exists(index=INDEX_NAME_1):
#     es.indices.create(index=INDEX_NAME_1, body=index_mapping_1)
#     print(f"Index '{INDEX_NAME_1}' created with text mapping and analyzer!")

# if not es.indices.exists(index=INDEX_NAME_2):
#     es.indices.create(index=INDEX_NAME_2, body=index_mapping_2)
#     print(f"Index '{INDEX_NAME_2}' created with text mapping and analyzer!")


# def index_documents_bulk(news_data, index_name):
#     """
#     Index multiple documents in Elasticsearch using the bulk API.
#     """
#     actions = [
#         {
#             "_index": index_name,
#             "_id": str(news['id']),
#             "_source": {"text": news['details_content'] if index_name == INDEX_NAME_2 else news['content']}
#         }
#         for news in news_data
#     ]
#     helpers.bulk(es, actions)
#     print(f"Indexed {len(actions)} documents successfully!")


# def fetch_news_data(table, db_name):
#     """
#     Fetch all news data from the MySQL database.
#     """
#     db_connection = pymysql.connect(
#         host="localhost",
#         user="root",
#         password="",
#         database=db_name
#     )

#     query = f"SELECT id, content FROM {table}" if table == 'trends' else f"SELECT id, details_content FROM {table}"
    
#     try:
#         with db_connection.cursor(pymysql.cursors.DictCursor) as cursor:
#             cursor.execute(query)
#             return cursor.fetchall()
#     except Exception as e:
#         print(f"Error fetching data from MySQL: {e}")
#         return []
#     finally:
#         db_connection.close()


# def search_matching_documents(search_terms, index_name):
#     """
#     Searches for documents matching the specified terms.
#     """
#     should_clauses = [{"match": {"text": term}} for term in search_terms]

#     response = es.search(index=index_name, body={
#         "query": {
#             "bool": {
#                 "should": should_clauses,
#                 "minimum_should_match": 1  # At least one term should match
#             }
#         }
#     })

#     matching_ids = [hit["_id"] for hit in response["hits"]["hits"]]
#     print(f"Matching IDs in {index_name}: {matching_ids}")
#     return matching_ids


# def get_news_data_by_ids(ids, db_name):
#     """
#     Fetches the full news data from the MySQL database based on the given list of IDs.
#     """
#     if not ids:
#         return []

#     db_connection = pymysql.connect(
#         host="localhost",
#         user="root",
#         password="",
#         database=db_name
#     )

#     try:
#         with db_connection.cursor(pymysql.cursors.DictCursor) as cursor:
#             query = f"""
#                 SELECT id, datetime, agency, title, details_link, details_content, sentiment, summary, category_id, type, status, created_at, updated_at 
#                 FROM news 
#                 WHERE id IN ({','.join(['%s'] * len(ids))})
#             """
#             cursor.execute(query, tuple(ids))  # Pass the tuple here
#             return cursor.fetchall()
#     except Exception as e:
#         print(f"Error fetching news data: {e}")
#         return []
#     finally:
#         db_connection.close()


# def index_news_from_db(table, db_name, index_name):
#     """
#     Fetch and index news data from MySQL into Elasticsearch.
#     """
#     news_data = fetch_news_data(table, db_name)
#     if news_data:
#         index_documents_bulk(news_data, index_name)
#     else:
#         print(f"No news data found to index from {table}.")


# def get_matching_facebook_documents(search_terms):
#     """
#     Fetch matching documents from Elasticsearch and MySQL for both indices.
#     """
#     # Fetch and index news data for both indices (only needs to be done once or when data changes)
#     index_news_from_db('trends', 'estate', INDEX_NAME_1)
#     index_news_from_db('news', 'aibot_lic_update', INDEX_NAME_2)

#     # Perform the search on both indices
#     matching_ids_1 = search_matching_documents(search_terms, INDEX_NAME_1)
#     matching_ids_2 = search_matching_documents(search_terms, INDEX_NAME_2)

#     # Combine the matching IDs and fetch the data from MySQL
#     matching_ids = matching_ids_1 + matching_ids_2
#     if matching_ids:
#         news_data = get_news_data_by_ids(matching_ids, 'aibot_lic_update')
#         return {"matching_documents": news_data} if news_data else {"message": "No detailed news data found for the matching IDs."}
#     else:
#         return {"message": "No matches found in Elasticsearch."}


# # Example: Search for documents
# search_terms = ["তারেক"]  # Add your Bengali search terms
# matching_data = get_matching_facebook_documents(search_terms)

# print(matching_data)


import pymysql
from elasticsearch import Elasticsearch, helpers

# Connect to Elasticsearch
es = Elasticsearch("http://localhost:9200")

# Index names for the two sources
INDEX_NAME_1 = "global_texts_facebook"
INDEX_NAME_2 = "global_texts_news"

# Define the index mapping with a suitable analyzer for text
index_mapping = {
    "mappings": {
        "properties": {
            "text": {
                "type": "text",
                "analyzer": "standard",  # Use "bengali_analyzer" if configured
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

# Ensure the Elasticsearch indices exist
for index in [INDEX_NAME_1, INDEX_NAME_2]:
    if not es.indices.exists(index=index):
        es.indices.create(index=index, body=index_mapping)
        print(f"Index '{index}' created with text mapping and analyzer!")

def index_documents_bulk(news_data, index_name):
    """
    Index multiple documents in Elasticsearch using the bulk API for a specific index.
    """
    actions = [
        {
            "_index": index_name,
            "_id": str(news['id']),
            "_source": {"text": news['content'] if index_name == INDEX_NAME_1 else news['details_content']}
        }
        for news in news_data
    ]
    helpers.bulk(es, actions)
    print(f"Indexed {len(actions)} documents successfully into {index_name}!")

def fetch_facebook_data():
    """
    Fetch all Facebook data from the MySQL estate database (trends table).
    """
    db_connection = pymysql.connect(
        host="localhost",
        user="root",
        password="",
        database="estate"
    )
    query = "SELECT id, content FROM trends"
    
    try:
        with db_connection.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute(query)
            return cursor.fetchall()
    except Exception as e:
        print(f"Error fetching data from MySQL estate: {e}")
        return []
    finally:
        db_connection.close()

def fetch_news_data():
    """
    Fetch all news data from the MySQL aibot_lic_update database (news table).
    """
    db_connection = pymysql.connect(
        host="localhost",
        user="root",
        password="",
        database="aibot_lic_update"
    )
    query = "SELECT id, details_content FROM news"
    
    try:
        with db_connection.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute(query)
            return cursor.fetchall()
    except Exception as e:
        print(f"Error fetching data from MySQL aibot_lic_update: {e}")
        return []
    finally:
        db_connection.close()

def search_matching_documents(search_terms, index_name):
    """
    Searches for documents matching the specified terms from the given index.
    """
    should_clauses = [{"match": {"text": term}} for term in search_terms]

    response = es.search(index=index_name, body={
        "query": {
            "bool": {
                "should": should_clauses,
                "minimum_should_match": 1  # At least one term should match
            }
        }
    })

    matching_ids = [hit["_id"] for hit in response["hits"]["hits"]]
    print(f"Matching IDs from {index_name}: {matching_ids}")
    return matching_ids

def get_data_by_ids(ids, source):
    """
    Fetches the full data from MySQL based on the given list of IDs for a specific source (either 'news' or 'facebook').
    """
    if not ids:
        return []

    db_connection = pymysql.connect(
        host="localhost",
        user="root",
        password="",
        database="aibot_lic_update" if source == "news" else "estate"
    )

    try:
        with db_connection.cursor(pymysql.cursors.DictCursor) as cursor:
            if source == "news":
                query = f"SELECT * FROM news WHERE id IN ({','.join(['%s'] * len(ids))})"
            elif source == "facebook":
                query = f"SELECT * FROM trends WHERE id IN ({','.join(['%s'] * len(ids))})"
            cursor.execute(query, tuple(ids))
            return cursor.fetchall()
    except Exception as e:
        print(f"Error fetching data: {e}")
        return []
    finally:
        db_connection.close()

def index_data_from_databases():
    """
    Fetch and index data from both the Facebook (estate) and News (aibot_lic_update) databases into their respective Elasticsearch indices.
    """
    facebook_data = fetch_facebook_data()
    if facebook_data:
        index_documents_bulk(facebook_data, INDEX_NAME_1)
    else:
        print("No Facebook data found to index.")

    news_data = fetch_news_data()
    if news_data:
        index_documents_bulk(news_data, INDEX_NAME_2)
    else:
        print("No news data found to index.")

def get_matching_documents_from_both(search_terms):
    """
    Fetch matching documents from both Elasticsearch indices and MySQL.
    This function returns separate results for 'news' and 'facebook' data.
    """
    matching_ids_facebook = search_matching_documents(search_terms, INDEX_NAME_1)
    matching_ids_news = search_matching_documents(search_terms, INDEX_NAME_2)

    categorized_results = {"facebook_data": [], "news_data": []}

    if matching_ids_facebook:
        facebook_data = get_data_by_ids(list(matching_ids_facebook), "facebook")
        categorized_results["facebook_data"] = facebook_data

    if matching_ids_news:
        news_data = get_data_by_ids(list(matching_ids_news), "news")
        categorized_results["news_data"] = news_data

    if categorized_results["facebook_data"] or categorized_results["news_data"]:
        return categorized_results
    else:
        return {"message": "No matches found in Elasticsearch."}

# Index data from both databases (only needs to be done once or when data changes)
index_data_from_databases()

# # Example: Search for documents
# search_terms = ["bangladesh"]  # Add your Bengali search terms
# matching_data = get_matching_documents_from_both(search_terms)

# # print(matching_data)

# facebook_data=matching_data['facebook_data']


# news_data=matching_data["news_data"]






