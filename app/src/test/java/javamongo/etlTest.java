package javamongo;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class etlTest {

    private MongoDatabase treeDB;
    private Etl etl;

    public void setup() {
        MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017/");
        MongoDatabase treeDB = mongoClient.getDatabase("VancouverOpenData_Trees");
        this.treeDB = treeDB;

        List<Document> docs = new ArrayList<>();

        //Normal
        Document doc1 = new Document("_id", 1);
        doc1.append("genus_name", "PLATANUS");
        doc1.append("common_name", "LONDON PLANE TREE");
        doc1.append("tree_id", 1);
        docs.add(doc1);

        //Normal
        Document doc2 = new Document("_id", 2);
        doc2.append("genus_name", "ACER");
        doc2.append("common_name", "NORWAY MAPLE");
        doc2.append("tree_id", 2);
        docs.add(doc2);

        //Empty
        Document doc3 = new Document("_id", 3);
        docs.add(doc3);

        //Empty common
        Document doc4 = new Document("_id", 4);
        doc4.append("genus_name", "ACER");
        doc4.append("common_name", "");
        doc4.append("tree_id", 4);
        docs.add(doc4);

        //No common
        Document doc5 = new Document("_id", 5);
        doc5.append("genus_name", "ACER");
        doc5.append("tree_id", 5);
        docs.add(doc5);

        //Empty genus
        Document doc6 = new Document("_id", 6);
        doc6.append("genus_name", "");
        doc6.append("common_name", "NORWAY MAPLE");
        doc6.append("tree_id", 6);
        docs.add(doc6);

        //No genus
        Document doc7 = new Document("_id", 7);
        doc7.append("common_name", "NORWAY MAPLE");
        doc7.append("tree_id", 7);
        docs.add(doc7);

        //No either
        Document doc8 = new Document("_id", 8);
        doc8.append("tree_id", 8);
        docs.add(doc8);

        //No tree_id
        Document doc9 = new Document("_id", 9);
        doc9.append("genus_name", "PLATANUS");
        doc9.append("common_name", "LONDON PLANE TREE");
        docs.add(doc9);

        //Non-english
        Document doc10 = new Document("_id", 10);
        doc10.append("genus_name", "木");
        doc10.append("common_name", "Français");
        doc10.append("tree_id", 10);
        docs.add(doc10);


        treeDB.getCollection("testTrees").drop();
        MongoCollection<Document> testColl = treeDB.getCollection("testTrees");
        testColl.insertMany(docs);

        this.etl = new Etl(treeDB, false);
    }

    @Test
    void emptyCollection() {
        etl.rebuildTreeFields("", "testFields");
        Long result = treeDB.getCollection("testFields").countDocuments();

        assertEquals(0, result);

    }
}
