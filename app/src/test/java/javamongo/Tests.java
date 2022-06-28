package javamongo;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.regex.Pattern;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import static com.mongodb.client.model.Filters.*;

public class Tests {

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
        assertEquals(6, count);
    }

    @Test
    void verifyingMapKeyValues() {
        Etl testMapEtl = new Etl(testTreeDB, true);
        testMapEtl.rebuildTreeFields("testSubTrees", "testSubTreesFields");

        MongoCollection<Document> testTreeFields = testTreeDB.getCollection("testSubTreesFieldsFriendlyMap");

        long count = testTreeFields.countDocuments();
        assertEquals(362, count);

        FindIterable<Document> norwayMapleKV = testTreeFields.find(eq("key", "ACER:SILVER VARIEGATED NORWAY MAPLE"));
        assertFalse(norwayMapleKV.first().isEmpty());
    }

    @Test
    void erroneousGeomDocuments() {
        Document noFriendly = new Document("_id", 100);
        noFriendly.append("test_comment", "No Friendly Name");
        noFriendly.append("genus_name", "MUSA");
        noFriendly.append("common_name", "BANANA TREE");
        noFriendly.append("tree_id", 100);
        noFriendly.append("geom", new Document("coordinates", Arrays.asList(-125.17067, 39.25875)));

        testEtl.rebuildTreeFields("testTrees", "testTreeFields");
        MongoCollection<Document> testTreeFields = testTreeDB.getCollection("testTreeFields");

        testTreeFields.insertOne(noFriendly);

        TreeQueries testTQ = new TreeQueries(testTreeDB, "testTreeFields");
        testTQ.groupByFriendlyName("testFriendlyNameCount");

        MongoCollection<Document> testFriendlyNameCount = testTreeDB.getCollection("testFriendlyNameCount");
        AggregateIterable<Document> totalTrees = testFriendlyNameCount.aggregate(
                Arrays.asList(new Document("$group",
                        new Document("_id", "total")
                                .append("count", new Document("$sum", "$count")))));

        assertEquals(6, totalTrees.first().get("count"));

        // No friendly names not included
        long count = testFriendlyNameCount.countDocuments(eq("_id", "BANANA"));
        assertEquals(0, count);

        // Non-english characters work
        count = testFriendlyNameCount.countDocuments(eq("_id", "Français"));
        assertEquals(1, count);
    }

    @Test
    void friendlyBySectionBadValues() {
        testEtl.rebuildTreeFields("testTrees", "testTreeFields");
        testTreeDB.getCollection("testTreesInSection").drop();

        TreeQueries testTQ = new TreeQueries(testTreeDB, "testTreeFields");

        testTQ.friendlyNameBySection(0, -3, "testTreesInSection");
        MongoCollection<Document> testTreesInSection = testTreeDB.getCollection("testTreesInSection");
        long count = testTreesInSection.countDocuments();
        assertEquals(0, count);
    }

    @Test
    void friendlyBySectionErroneous() {
        testEtl.rebuildTreeFields("testTrees", "testTreeFields");
        testTreeDB.getCollection("testTreesInSection").drop();

        TreeQueries testTQ = new TreeQueries(testTreeDB, "testTreeFields");

        testTQ.friendlyNameBySection(2, 3, "testTreesInSection");
        MongoCollection<Document> testTreesInSection = testTreeDB.getCollection("testTreesInSection");
        long totalSectionsCount = testTreesInSection.countDocuments();
        assertEquals(6, totalSectionsCount);

        AggregateIterable<Document> totalTrees = testTreesInSection.aggregate(
                Arrays.asList(
                        new Document("$match",
                                new Document("trees_by_section.count",
                                        new Document("$exists", 1)))));

        MongoCursor<Document> it = totalTrees.iterator();
        int totalSectionsWithTreesCount = 0;
        while (it.hasNext()) {
            totalSectionsWithTreesCount++;
            it.next();
        }
        assertEquals(2, totalSectionsWithTreesCount);

        int appleCount = 0;
        totalTrees = testTreesInSection.aggregate(
                Arrays.asList(
                        new Document("$match",
                                new Document("trees_by_section.count",
                                        new Document("$exists", 1))
                                        .append("trees_by_section._id", "APPLE")
                                        .append("trees_by_section.count", 1))));

        it = totalTrees.iterator();
        while (it.hasNext()) {
            appleCount++;
            it.next();
        }
        assertEquals(1, appleCount);

    }

    @Test
    void friendlyNameByGenus() {
        testEtl.rebuildTreeFields("testSubTrees", "testSubTreesFields");
        Bson query = eq("friendly_name", "ASH");
        long count = testTreeDB.getCollection("testSubTreesFields").countDocuments(query);
        assertEquals(206, count);
    }

    @Test
    void friendlyNameByCommon() {
        testEtl.rebuildTreeFields("testSubTrees", "testSubTreesFields");
        Bson query = eq("friendly_name", "MAPLE");
        long count = testTreeDB.getCollection("testSubTreesFields").countDocuments(query);
        assertEquals(1268, count);
    }

    @Test
    void friendlyNameByCommonSpecies() {
        testEtl.rebuildTreeFields("testSubTrees", "testSubTreesFields");

        MongoCollection<Document> testSubTreesFields = testTreeDB.getCollection("testSubTreesFields");
        AggregateIterable<Document> spruceSpeciesTrees = testSubTreesFields.aggregate(Arrays.asList(new Document("$match",
                new Document("friendly_name", "SPRUCE")),
                new Document("$match",
                        new Document("common_name",
                        new Document("$regex", Pattern.compile("SPECIES"))))));
        
        MongoCursor<Document> it = spruceSpeciesTrees.iterator();
        int spruceSpeciesTreesCount = 0;
        while (it.hasNext()) {
            spruceSpeciesTreesCount++;
            it.next();
        }
        assertEquals(4, spruceSpeciesTreesCount);
    }

    @AfterAll
    private static void cleanup() {
        // testTreeDB.getCollection("testTreeFields").deleteOne(eq("_id", 100));

        // drop every test collection
        testTreeDB.getCollection("testFriendlyNameCount").drop();
        testTreeDB.getCollection("testSubTreesFields").drop();
        testTreeDB.getCollection("testSubTreesFieldsFriendlyMap").drop();
        testTreeDB.getCollection("testTreeFields").drop();
        testTreeDB.getCollection("testTreeFieldsFriendlyMap").drop();
        
    }
}
