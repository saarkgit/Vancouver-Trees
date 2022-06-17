package javamongo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

// ETL (Extract, Transform, Load)
public class Etl {
    private MongoDatabase db;
    private Map<String, String> map;
    private boolean updateMapColl;

    Etl(MongoDatabase mdb, boolean updateMap) {
        db = mdb;
        map = new HashMap<>();
        updateMapColl = updateMap;
    }

    public void rebuildTreeFields() {
        MongoCollection<Document> treeDataCollection = db.getCollection("Trees");
        AggregateIterable<Document> fieldsList = treeDataCollection
                .aggregate(Arrays.asList(new Document("$replaceRoot",
                        new Document("newRoot", "$fields"))));

        db.getCollection("TreeFields").drop();
        MongoCollection<Document> treeFieldsData = db.getCollection("TreeFields");

        List<Document> listOfFields = new ArrayList<>();

        MongoCollection<Document> friendlyNameMapColl = db.getCollection("friendlyMap");
        if (friendlyNameMapColl.countDocuments() > 0) {
            FindIterable<Document> mapEntrys = friendlyNameMapColl.find();

            for (Document document : mapEntrys) {
                map.put(document.get("key").toString(), document.get("value").toString());
            }
        }
        
        for (Document document : fieldsList) {
            String genus = document.get("genus_name").toString();
            String common = document.get("common_name").toString();
            String friendly = getFriendlyName(genus, common);
            String id = document.get("tree_id").toString();
            document.append("_id", id);
            document.append("friendly_name", friendly);
            listOfFields.add(document);
        }

        treeFieldsData.insertMany(listOfFields);

        if(updateMapColl) {
            rebuildTreeFieldsMap();
        }
    }

    private String getFriendlyName(String genus, String common) {
        String friendlyKey = genus + ":" + common;
        String friendlyValue = "";

        if (map.containsKey(friendlyKey)) {
            friendlyValue = map.get(friendlyKey);
        } else {
            String friendlyName = "";

            if (common.equals("BAUMANN'S SEEDLESS HORSECHESTN")) {
                friendlyName = "HORSECHESTNUT";
            } else if (common.equals("CHERRY, PLUM OR PEACH SPECIES")) {
                friendlyName = "PRUNUS";
            } else {
                friendlyName = matchGenus(genus, common);
            }

            String[] words = friendlyName.split(" ");
            String last = words[words.length - 1];

            if (last.equals("TREE")) {
                last = decipherTreeSuffix(words);
            }

            int i = 2;
            while (last.equals("SPECIES")) {
                last = words[words.length - i];
                i++;
            }

            friendlyValue = last;
            map.put(friendlyKey, friendlyValue);
        }
        return friendlyValue;
    }

    private String matchGenus(String genus, String common) {
        String result = common;
        switch (genus) {
            case "MAGNOLIA":
                result = "MAGNOLIA";
                break;
            case "FRAXINUS":
                result = "ASH";
                break;
            case "THUJA":
                result = "CEDAR";
                break;
            case "CHAMAECYPARIS":
                result = "CYPRESS";
                break;
            case "ACER":
                result = "MAPLE";
                break;
        }
        return result;
    }

    private String decipherTreeSuffix(String[] common) {
        String treeFriendlyName = common[common.length - 2];

        switch (treeFriendlyName) {
            case "CHAIN":
                treeFriendlyName = "GOLDENCHAIN";
                break;
            case "LOTUS":
                treeFriendlyName = "MAGNOLIA";
                break;
        }

        return treeFriendlyName;
    }

    private void rebuildTreeFieldsMap() {
        MongoCollection<Document> friendlyNameMapColl = db.getCollection("friendlyMap");

        db.getCollection("friendlyMap").drop();
        friendlyNameMapColl = db.getCollection("friendlyMap");

        List<Document> listOfNames = new ArrayList<>();

        for (Map.Entry<String, String> entry : map.entrySet()) {
            Document doc = new Document();
            doc.append("key", entry.getKey());
            doc.append("value", entry.getValue());
            listOfNames.add(doc);
        }

        friendlyNameMapColl.insertMany(listOfNames);
    }

}
