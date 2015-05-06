/**
 * MongoDB client binding for YCSB.
 *
 * Submitted by Yen Pai on 5/11/2010.
 *
 * https://gist.github.com/000a66b8db2caf42467b#file_mongo_db.java
 *
 */

package com.yahoo.ycsb.db;

import com.google.gson.Gson;
import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.Database;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.DocumentCollection;

import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * MongoDB client for YCSB framework.
 * 
 * Properties to set:
 * 
 * mongodb.url=mongodb://localhost:27017 mongodb.database=ycsb
 * mongodb.writeConcern=normal
 * 
 * @author ypai
 */
public class DocumentDbClient extends DB {

    /** DocumentDB-specific values 
    private static final String END_POINT = "[YOUR_ENDPOINT_HERE]";
    private static final String MASTER_KEY = "[YOUR_KEY_HERE]";
    private static final String DATABASE_ID = "ycsb";
    private static final String COLLECTION_ID = "benchmarking";*/

    /** We'll use Gson for POJO <=> JSON serialization */
    private static Gson gson = new Gson();

    /**
     * Initialize any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    @Override
    public void init() throws DBException {

        Properties props = getProperties();
        String endpoint = props.getProperty("docdb.endpoint",
                "??? What goes here?");
        String masterkey = props.getProperty("docdb.masterkey",
                "??? What goes here?")
        String database = props.getProperty("docdb.database", "ycsb");
        String collection = props.getProperty("docdb.collection", "benchmarking")

        try {
            // Instantiate a DocumentClient w/ provided DocumentDB Endpoint and AuthKey.
            DocumentClient documentClient = new DocumentClient(endpoint,
                    masterkey, ConnectionPolicy.GetDefault(), ConsistencyLevel.Session);

            // Define a new database using the id above.
            Database myDatabase = new Database();
            myDatabase.setId(database);

            // Create a new database.
            myDatabase = documentClient.createDatabase(myDatabase, null)
                    .getResource();

            // Define a new collection using the id above.
            DocumentCollection myCollection = new DocumentCollection();
            myCollection.setId(collection);

            // Configure the new collection performance tier to S1.
            RequestOptions requestOptions = new RequestOptions();
            requestOptions.setOfferType("S1");

            // Create a new collection.
            myCollection = documentClient.createCollection(
                    myDatabase.getSelfLink(), myCollection, requestOptions).getResource();

            System.out.println("documentdb connection created with " + url);
        }
        catch (Exception e1) {
            System.err
                    .println("Could not initialize DocDB connection pool for Loader: "
                            + e1.toString());
            e1.printStackTrace();
            return;
        }
    }

    /**
     * Cleanup any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    @Override
    public void cleanup() throws DBException {
        if (initCount.decrementAndGet() <= 0) {
            try {
                mongo.close();
            }
            catch (Exception e1) {
                System.err.println("Could not close MongoDB connection pool: "
                        + e1.toString());
                e1.printStackTrace();
                return;
            }
        }
    }

    /**
     * Delete a record from the database.
     *
     * @param table The name of the table
     * @param key The record key of the record to delete.
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    @Override
    public int delete(String table, String key) {
        com.mongodb.DB db = null;
        try {
            db = mongo.getDB(database);
            db.requestStart();
            DBCollection collection = db.getCollection(table);
            DBObject q = new BasicDBObject().append("_id", key);
            WriteResult res = collection.remove(q, writeConcern);
            return res.getN() == 1 ? 0 : 1;
        }
        catch (Exception e) {
            System.err.println(e.toString());
            return 1;
        }
        finally {
            if (db != null) {
                db.requestDone();
            }
        }
    }

    /**
     * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
     * record key.
     *
     * @param table The name of the table
     * @param key The record key of the record to insert.
     * @param values A HashMap of field/value pairs to insert in the record
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    @Override
    public int insert(String table, String key,
            HashMap<String, ByteIterator> values) {
        com.mongodb.DB db = null;
        try {
            db = mongo.getDB(database);

            db.requestStart();

            DBCollection collection = db.getCollection(table);
            DBObject r = new BasicDBObject().append("_id", key);
            for (String k : values.keySet()) {
                r.put(k, values.get(k).toArray());
            }
            WriteResult res = collection.insert(r, writeConcern);
            return res.getError() == null ? 0 : 1;
        }
        catch (Exception e) {
            e.printStackTrace();
            return 1;
        }
        finally {
            if (db != null) {
                db.requestDone();
            }
        }
    }

    /**
     * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
     *
     * @param table The name of the table
     * @param key The record key of the record to read.
     * @param fields The list of fields to read, or null for all of them
     * @param result A HashMap of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error or "not found".
     */
    @Override
    @SuppressWarnings("unchecked")
    public int read(String table, String key, Set<String> fields,
            HashMap<String, ByteIterator> result) {
        com.mongodb.DB db = null;
        try {
            db = mongo.getDB(database);

            db.requestStart();

            DBCollection collection = db.getCollection(table);
            DBObject q = new BasicDBObject().append("_id", key);
            DBObject fieldsToReturn = new BasicDBObject();

            DBObject queryResult = null;
            if (fields != null) {
                Iterator<String> iter = fields.iterator();
                while (iter.hasNext()) {
                    fieldsToReturn.put(iter.next(), INCLUDE);
                }
                queryResult = collection.findOne(q, fieldsToReturn);
            }
            else {
                queryResult = collection.findOne(q);
            }

            if (queryResult != null) {
                result.putAll(queryResult.toMap());
            }
            return queryResult != null ? 0 : 1;
        }
        catch (Exception e) {
            System.err.println(e.toString());
            return 1;
        }
        finally {
            if (db != null) {
                db.requestDone();
            }
        }
    }

    /**
     * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
     * record key, overwriting any existing values with the same field name.
     *
     * @param table The name of the table
     * @param key The record key of the record to write.
     * @param values A HashMap of field/value pairs to update in the record
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    @Override
    public int update(String table, String key,
            HashMap<String, ByteIterator> values) {
        com.mongodb.DB db = null;
        try {
            db = mongo.getDB(database);

            db.requestStart();

            DBCollection collection = db.getCollection(table);
            DBObject q = new BasicDBObject().append("_id", key);
            DBObject u = new BasicDBObject();
            DBObject fieldsToSet = new BasicDBObject();
            Iterator<String> keys = values.keySet().iterator();
            while (keys.hasNext()) {
                String tmpKey = keys.next();
                fieldsToSet.put(tmpKey, values.get(tmpKey).toArray());

            }
            u.put("$set", fieldsToSet);
            WriteResult res = collection.update(q, u, false, false,
                    writeConcern);
            return res.getN() == 1 ? 0 : 1;
        }
        catch (Exception e) {
            System.err.println(e.toString());
            return 1;
        }
        finally {
            if (db != null) {
                db.requestDone();
            }
        }
    }

    /**
     * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored in a HashMap.
     *
     * @param table The name of the table
     * @param startkey The record key of the first record to read.
     * @param recordcount The number of records to read
     * @param fields The list of fields to read, or null for all of them
     * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    @Override
    public int scan(String table, String startkey, int recordcount,
            Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        com.mongodb.DB db = null;
        try {
            db = mongo.getDB(database);
            db.requestStart();
            DBCollection collection = db.getCollection(table);
            // { "_id":{"$gte":startKey, "$lte":{"appId":key+"\uFFFF"}} }
            DBObject scanRange = new BasicDBObject().append("$gte", startkey);
            DBObject q = new BasicDBObject().append("_id", scanRange);
            DBCursor cursor = collection.find(q).limit(recordcount);
            while (cursor.hasNext()) {
                // toMap() returns a Map, but result.add() expects a
                // Map<String,String>. Hence, the suppress warnings.
                HashMap<String, ByteIterator> resultMap = new HashMap<String, ByteIterator>();

                DBObject obj = cursor.next();
                fillMap(resultMap, obj);

                result.add(resultMap);
            }

            return 0;
        }
        catch (Exception e) {
            System.err.println(e.toString());
            return 1;
        }
        finally {
            if (db != null) {
                db.requestDone();
            }
        }

    }

    /**
     * TODO - Finish
     * 
     * @param resultMap
     * @param obj
     */
    @SuppressWarnings("unchecked")
    protected void fillMap(HashMap<String, ByteIterator> resultMap, DBObject obj) {
        Map<String, Object> objMap = obj.toMap();
        for (Map.Entry<String, Object> entry : objMap.entrySet()) {
            if (entry.getValue() instanceof byte[]) {
                resultMap.put(entry.getKey(), new ByteArrayByteIterator(
                        (byte[]) entry.getValue()));
            }
        }
    }

    /**
     * Get a specified DocumentDB database, or create one if it doesn't exist
     *
     * @return The created Database object
     */
    private Database getDatabase() {
        Database db;

        // Get the database if it exists
        List<Database> databaseList = documentClient
                .queryDatabases(
                        "SELECT * FROM root r WHERE r.id='" + database
                                + "'", null).getQueryIterable().toList();

        if (databaseList.size() > 0) {
            // Cache the database object so we won't have to query for it
            // later to retrieve the selfLink.
            db = databaseList.get(0);
        } else {
            // Create the database if it doesn't exist.
            try {
                Database databaseDefinition = new Database();
                databaseDefinition.setId(database);

                db = documentClient.createDatabase(
                        databaseDefinition, null).getResource();
            } catch (DocumentClientException e) {
                // TODO: Something has gone terribly wrong - the app wasn't
                // able to query or create the collection.
                // Verify your connection, endpoint, and key.
                e.printStackTrace();
            }
        }

        return db;
    }

    /**
     * Get a specified DocumentDB document collection, or create one if it doesn't exist
     *
     * @return The created document Collection object
     */
    private DocumentCollection getCollection() {
        DocumentCollection coll;

        // Get the collection if it exists.
        List<DocumentCollection> collectionList = documentClient
                .queryCollections(
                        getDatabase().getSelfLink(),
                        "SELECT * FROM root r WHERE r.id='" + collection
                                + "'", null).getQueryIterable().toList();

        if (collectionList.size() > 0) {
            // Cache the collection object so we won't have to query for it
            // later to retrieve the selfLink.
            coll = collectionList.get(0);
        } else {
            // Create the collection if it doesn't exist.
            try {
                DocumentCollection collectionDefinition = new DocumentCollection();
                collectionDefinition.setId(collection);

                coll = documentClient.createCollection(
                        getDatabase().getSelfLink(),
                        collectionDefinition, null).getResource();
            } catch (DocumentClientException e) {
                // TODO: Something has gone terribly wrong - the app wasn't
                // able to query or create the collection.
                // Verify your connection, endpoint, and key.
                e.printStackTrace();
            }
        }

        return coll;
    }
}
