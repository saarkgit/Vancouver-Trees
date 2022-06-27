package javamongo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.function.Executable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class etlTest {

    private static MongoDatabase testTreeDB;
    private static Etl testEtl;

    @BeforeAll
    private static void setup() {
        MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017/");
        MongoDatabase treeDB = mongoClient.getDatabase("VancouverOpenData_Trees");
        testTreeDB = treeDB;
        testEtl = new Etl(treeDB, false);
    }

    @Test
    void emptyCollection() {
        testEtl.rebuildTreeFields("test", "testTreeFields");
        long count = testTreeDB.getCollection("testTreeFields").countDocuments();
        assertEquals(0, count);
    }

    @Test
    void erroneousDocuments() {
        testEtl.rebuildTreeFields("testTrees", "testTreeFields");
        long count = testTreeDB.getCollection("testTreeFields").countDocuments();
        assertEquals(3, count);
    }

    @Test
    void friendlyNameByGenus() {
    testEtl.rebuildTreeFields("testSubTrees", "testSubTreesFields");
    long count = testTreeDB.getCollection("testSubTreesFields").countDocuments();
    assertEquals(5000, count);
    }

    // @Test
    // void erroneousGeomDocuments() {

    // }
}
