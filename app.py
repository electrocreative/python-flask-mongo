"""
A simple application using a Python, Flask and MongoDB stack with ReST API
endpoint examples for typical C(reate), R(ead), U(pdate), & D(elete) operations.

Based on: https://ishmeet1995.medium.com/how-to-create-restful-crud-api-with-
python-flask-mongodb-and-docker-8f6ccb73c5bc

Author: Scott R. Henz
Date:   12/30/2022
"""
from flask import Flask, request, json, Response
from pymongo import MongoClient
import logging as log

app = Flask(__name__)


# *** DEFINE MAIN CLASS METHODS ***
class MongoAPI:
    def __init__(self, data):
        """Define the database connection & __init__ method."""
        self.client = MongoClient("mongodb://localhost:27017/")
        database = data['database']
        collection = data['collection']
        cursor = self.client[database]
        self.collection = cursor[collection]
        self.data = data

    def create(self, data):
        """Method to create (write/insert) a new record and return status."""
        log.info('Writing Data')
        new_document = data['Document']
        response = self.collection.insert_one(new_document)
        output = {'Status': 'Successfully Inserted',
                  'Document_ID': str(response.inserted_id)}
        return output

    def read(self):
        """Method to read and return all records."""
        log.info('Reading All Data')
        documents = self.collection.find()
        output = [{item: data[item] for item in data if item != '_id'} for data
                  in documents]
        return output

    def update(self):
        """Method to select and update a record and return status."""
        log.info('Updating Data')
        fltr = self.data['Filter']
        updated_data = {"$set": self.data['DataToBeUpdated']}
        response = self.collection.update_one(fltr, updated_data)
        output = {
            'Status': 'Successfully Updated' if response.modified_count > 0
            else "Nothing was updated."}
        return output

    def delete(self, data):
        """Method to select and delete a record and return status."""
        log.info('Deleting Data')
        fltr = data['Filter']
        response = self.collection.delete_one(fltr)
        output = {
            'Status': 'Successfully Deleted' if response.deleted_count > 0
            else "Document not found."}
        return output


# *** FLASK ROUTES ***
# Define 'Health' endpoint route.
@app.route('/health')
def base():
    return Response(response=json.dumps({"Status": "UP"}),
                    status=200,
                    mimetype='application/json')


# Define 'Create' endpoint route.
@app.route('/create', methods=['POST'])
def mongo_create():
    data = request.json
    if data is None or data == {} or 'Document' not in data:
        return Response(response=json.dumps({"Error": "Please provide "
                                                      "connection "
                                                      "information"}),
                        status=400,
                        mimetype='application/json')
    obj1 = MongoAPI(data)
    response = obj1.create(data)
    return Response(response=json.dumps(response),
                    status=200,
                    mimetype='application/json')


# Define 'Read' endpoint route.
@app.route('/read', methods=['GET'])
def mongo_read():
    data = request.json
    if data is None or data == {}:
        return Response(response=json.dumps({"Error": "Please provide "
                                                      "connection "
                                                      "information"}),
                        status=400,
                        mimetype='application/json')
    obj1 = MongoAPI(data)
    response = obj1.read()
    return Response(response=json.dumps(response),
                    status=200,
                    mimetype='application/json')


# Define 'Update' endpoint route.
@app.route('/update', methods=['PUT'])
def mongo_update():
    data = request.json
    if data is None or data == {} or 'DataToBeUpdated' not in data:
        return Response(response=json.dumps({"Error": "Please provide "
                                                      "connection "
                                                      "information"}),
                        status=400,
                        mimetype='application/json')
    obj1 = MongoAPI(data)
    response = obj1.update()
    return Response(response=json.dumps(response),
                    status=200,
                    mimetype='application/json')


# Define 'Delete' endpoint route.
@app.route('/delete', methods=['DELETE'])
def mongo_delete():
    data = request.json
    if data is None or data == {} or 'Filter' not in data:
        return Response(response=json.dumps({"Error": "Please provide "
                                                      "connection "
                                                      "information"}),
                        status=400,
                        mimetype='application/json')
    obj1 = MongoAPI(data)
    response = obj1.delete(data)
    return Response(response=json.dumps(response),
                    status=200,
                    mimetype='application/json')


# *** DEFINE RUNTIME PARAMS & LAUNCH ***
if __name__ == '__main__':
    app.run(debug=True, port=5001, host='0.0.0.0')
