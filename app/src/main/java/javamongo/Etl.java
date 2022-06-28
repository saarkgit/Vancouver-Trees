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

    public void rebuildTreeFields(String coll, String newColl) {
        db.getCollection(newColl).drop();
        MongoCollection<Document> finalColl = db.getCollection(newColl);
        MongoCollection<Document> treeDataCollection = db.getCollection(coll);

        if (treeDataCollection.countDocuments() == 0) {
            System.out.println("Source data collection is empty.");
            return;
        }

        Document match = new Document("$match",
                new Document("fields.tree_id", new Document("$exists", 1)));
        Document replaceRoot = new Document("$replaceRoot",
                new Document("newRoot", "$fields"));

        AggregateIterable<Document> fieldsList = treeDataCollection
                .aggregate(Arrays.asList(match, replaceRoot));

        List<Document> listOfFields = new ArrayList<>();

        String mapCollName = newColl + "friendlyMap";
        MongoCollection<Document> friendlyNameMapColl = db.getCollection(mapCollName);
        if (friendlyNameMapColl.countDocuments() > 0) {
            FindIterable<Document> mapEntrys = friendlyNameMapColl.find();

            for (Document document : mapEntrys) {
                map.put(document.get("key").toString(), document.get("value").toString());
            }
        }

        for (Document document : fieldsList) {
            Object genus = document.get("genus_name");
            Object common = document.get("common_name");
            Object id = document.get("tree_id");
            if (genus != null && !genus.toString().isEmpty() && common != null
                    && !common.toString().isEmpty() && id != null) {
                String friendly = getFriendlyName(genus.toString(), common.toString());
                String docId = (!id.toString().isEmpty()) ? id.toString() : document.get("_id").toString();
                document.append("_id", docId);
                document.append("friendly_name", friendly);
                listOfFields.add(document);
            }
        }

        finalColl.insertMany(listOfFields);

        if (updateMapColl) {
            rebuildTreeFieldsMap(mapCollName);
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

    private void rebuildTreeFieldsMap(String collMapName) {
        db.getCollection(collMapName).drop();
        MongoCollection<Document> friendlyNameMapColl = db.getCollection(collMapName);

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
