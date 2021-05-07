from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import ConnectionFailure

mongoDB_name = 'mongoDB_name'
# must include '&ssl_cert_reqs=CERT_NONE' at the end of connection string
mongoDB_connection_uri = 'mongoDB_connection_uri'
mongoDB_exclude_collection_names = ['excludeCollection']

documentDB_name = 'documentDB_name'
# must include '&ssl_cert_reqs=CERT_NONE' at the end of connection string
documentDB_connection_uri = 'documentDB_connection_uri'

all_collection_indexes = []


def get_collection_and_index():
    client = MongoClient(mongoDB_connection_uri)
    try:
        db = client[mongoDB_name]
        collection_names = db.list_collection_names()
        global all_collection_indexes
        print(collection_names)

        for collection_name in collection_names:
            if collection_name not in mongoDB_exclude_collection_names:
                collection = db[collection_name]
                indexes = collection.index_information()
                index_names = indexes.keys()
                converted_indexes = []

                for index_name in index_names:
                    index_value = indexes.get(index_name)['key']
                    converted_index_tuple_list = []
                    for index_tuple in index_value:
                        index_tuple_list = list(index_tuple)
                        if index_tuple_list[1] == 1 or index_tuple_list[1] == 1.0:
                            index_tuple_list[1] = ASCENDING
                        if index_tuple_list[1] == -1 or index_tuple_list[1] == -1.0:
                            index_tuple_list[1] = DESCENDING
                        converted_index_tuple_list.append(tuple(index_tuple_list))
                    converted_indexes.append({'index_name': index_name, 'index_value': converted_index_tuple_list})
                all_collection_indexes.append({'collection_name': collection_name, 'collection_indexes': converted_indexes})
        print(all_collection_indexes)
    except ConnectionFailure:
        print("Server not available")


def create_collection_and_index():
    client = MongoClient(documentDB_connection_uri)
    try:
        db = client[documentDB_name]
        for collection_info in all_collection_indexes:
            collection = db[collection_info['collection_name']]
            converted_indexes = collection_info['collection_indexes']
            index_number = 0
            for index in converted_indexes:
                index_name = "index_" + str(index_number)
                collection.create_index(index['index_value'], name=index_name)
                index_number += 1
            print("Create collection and indexes complete for " + collection_info['collection_name'])

    except ConnectionFailure:
        print("Server not available")


if __name__ == '__main__':
    get_collection_and_index()
    create_collection_and_index()

