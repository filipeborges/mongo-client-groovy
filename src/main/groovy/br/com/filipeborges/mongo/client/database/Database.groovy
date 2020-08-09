package br.com.filipeborges.mongo.client.database

import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.Filters
import com.mongodb.client.model.Projections
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
        collection.find()
                .projection(
                        Projections.fields(
                                Projections.include("f1", "f2", "f3", "f4"),
                                Projections.excludeId()
                        )
                )
                .each {
            data.add(it)
        }
        return data
    }

    def findFiltered(def filterData, String collectionName) {
        List<Document> data = []
        MongoCollection<Document> collection = db.getCollection(collectionName)
        collection.find(
                Filters.and(
                        Filters.in("f1", filterData["f1"]),
                        Filters.in("f2", filterData["f2"]),
                        Filters.in("f3", filterData["f3"]),
                        Filters.in("f4", filterData["f4"])
                )
        )
                .projection(
                        Projections.fields(
                                Projections.include("f1", "f2", "f3", "f4"),
                                Projections.excludeId()
                        )
                )
                .each { data.add(it) }
        System.out.println("TOTAL DOCUMENTS FINDED: ${data.size()}")
        return data
    }
}
