package javamongo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.google.gson.*;

import com.mongodb.BasicDBList;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import static com.mongodb.client.model.Sorts.*;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Projections.*;

import com.mongodb.client.model.*;

public class TreeQueries {
    private MongoDatabase db;
    private MongoCollection<Document> treeFieldsCollection;

    TreeQueries(MongoDatabase mdb) {
        db = mdb;
        treeFieldsCollection = db.getCollection("TreeFields");
    }

    public void groupByFriendlyName() {
        db.getCollection("friendlyNameCount").drop();

        AggregateIterable<Document> friendlyNameCursor = treeFieldsCollection.aggregate(
                Arrays.asList(
                        Aggregates.group("$friendly_name", Accumulators.sum("count", 1)),
                        Aggregates.sort(orderBy(descending("count"), ascending("_id")))));

        List<Document> friendlyNames = new ArrayList<>();

        for (Document document : friendlyNameCursor) {
            friendlyNames.add(document);
        }

        db.getCollection("friendlyNameCount").insertMany(friendlyNames);
    }

    public void diameterheightRatio() {
        db.getCollection("diamHeightR").drop();

        AggregateIterable<Document> friendlyNameCursor = treeFieldsCollection.aggregate(
                Arrays.asList(
                // Aggregates.group("$friendly_name", Accumulators.sum("count", 1))
                // Aggregates.sort(orderBy(descending("count"), ascending("_id")))
                ));

        List<Document> friendlyNames = new ArrayList<>();

        for (Document document : friendlyNameCursor) {
            friendlyNames.add(document);
        }

        db.getCollection("diamHeightR").insertMany(friendlyNames);
    }

    /*
     * diameter
     * plant area
     * height range
     * geom
     */
    public double[] geomFinder() {
        double geomMaxLong = Double.MIN_VALUE, geomMaxLat = Double.MIN_VALUE;
        double geomMinLong = Double.MIN_VALUE, geomMinLat = Double.MAX_VALUE;

        Bson match = match(and(
            exists("geom"), 
            lt("geom.coordinates.0", -122.5), 
            gt("geom.coordinates.0", -123.5),
            gt("geom.coordinates.1", 49), 
            lt("geom.coordinates.1", 50)
            ));
        Bson sort = sort(ascending("geom.coordinates.0"));
        Bson limit = limit(1);
        Bson project = project(fields(include("geom.coordinates"), excludeId()));
        Bson unwind = unwind("$geom.coordinates");
        
        Document doc = null;
        //get geomMinLong
        AggregateIterable<Document> geomCursor = treeFieldsCollection.aggregate(Arrays.asList(
            match, sort, limit, project, unwind)
            );
        doc = geomCursor.first();
        Document dbl = (Document) doc.get("geom");
        String str = dbl.toJson();
        JsonObject jsonObject = JsonParser.parseString(str).getAsJsonObject();
        geomMinLong = jsonObject.get("coordinates").getAsDouble();

        //get geomMaxLong
        sort = sort(descending("geom.coordinates.0"));
        geomCursor = treeFieldsCollection.aggregate(Arrays.asList(
            match, sort, limit, project, unwind)
            );
        doc = geomCursor.first();

        dbl = (Document) doc.get("geom");
        str = dbl.toJson();
        jsonObject = JsonParser.parseString(str).getAsJsonObject();
        geomMaxLong = jsonObject.get("coordinates").getAsDouble();

        //get geomMinLat
        sort = sort(ascending("geom.coordinates.1"));
        geomCursor = treeFieldsCollection.aggregate(Arrays.asList(
            match, sort, limit, project, unwind)
            );

        
        Iterator<Document> it = geomCursor.iterator();
        while(it.hasNext()){
           doc = it.next();
        }
        dbl = (Document) doc.get("geom");
        str = dbl.toJson();
        jsonObject = JsonParser.parseString(str).getAsJsonObject();
        geomMinLat = jsonObject.get("coordinates").getAsDouble();

        //get geomMaxLat
        sort = sort(descending("geom.coordinates.1"));
        geomCursor = treeFieldsCollection.aggregate(Arrays.asList(
            match, sort, limit, project, unwind)
            );

        it = geomCursor.iterator();
        while(it.hasNext()){
           doc = it.next();
        }

        dbl = (Document) doc.get("geom");
        str = dbl.toJson();
        jsonObject = JsonParser.parseString(str).getAsJsonObject();
        geomMaxLat = jsonObject.get("coordinates").getAsDouble();

        double[] result = { geomMaxLong, geomMinLong, geomMaxLat, geomMinLat };
        return result;
    }

}
