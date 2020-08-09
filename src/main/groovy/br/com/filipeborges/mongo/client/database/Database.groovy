package br.com.filipeborges.mongo.client.database

import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import org.bson.Document

class Database {

    private static MongoClient dbClient = null

    private MongoDatabase db = null

    Database() {
        if (!dbClient) dbClient = MongoClients.create("mongodb://localhost:27017")
        db = dbClient.getDatabase("stress-db")
    }

    long count(String collectionName) {
        MongoCollection<Document> collection = db.getCollection(collectionName)
        collection.countDocuments()
    }

    void close() {
        dbClient.close()
    }

    void printAll(String collectionName) {
        MongoCollection<Document> collection = db.getCollection(collectionName)
        collection.find().each {
            System.out.println(it)
        }
    }

    def getData(String collectionName) {
        List<Document> data = []
        MongoCollection<Document> collection = db.getCollection(collectionName)
        collection.find().each {
            data.add(it)
        }
        return data
    }
}
