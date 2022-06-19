package javamongo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.google.gson.*;

import com.mongodb.client.AggregateIterable;
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
        Bson sort = null;
        AggregateIterable<Document> geomCursor = null;
        Document doc = null;

        // get geomMinLong
        sort = sort(ascending("geom.coordinates.0"));
        geomCursor = geomFinderHelper(sort);
        doc = geomCursor.first();
        double geomMinLong = getCoordinate(doc);

        // get geomMaxLong
        sort = sort(descending("geom.coordinates.0"));
        geomCursor = geomFinderHelper(sort);
        doc = geomCursor.first();
        double geomMaxLong = getCoordinate(doc);

        // get geomMinLat
        sort = sort(ascending("geom.coordinates.1"));
        geomCursor = geomFinderHelper(sort);

        Iterator<Document> it = geomCursor.iterator();
        while (it.hasNext()) {
            doc = it.next();
        }

        double geomMinLat = getCoordinate(doc);

        // get geomMaxLat
        sort = sort(descending("geom.coordinates.1"));
        geomCursor = geomFinderHelper(sort);

        it = geomCursor.iterator();
        while (it.hasNext()) {
            doc = it.next();
        }

        double geomMaxLat = getCoordinate(doc);

        double[] result = { geomMaxLong, geomMinLong, geomMaxLat, geomMinLat };
        return result;
    }

    private AggregateIterable<Document> geomFinderHelper(Bson sort) {
        // Vancouver area
        Bson match = match(and(
                exists("geom"),
                lt("geom.coordinates.0", -122.5),
                gt("geom.coordinates.0", -123.5),
                gt("geom.coordinates.1", 49),
                lt("geom.coordinates.1", 50)));
        Bson limit = limit(1);
        Bson project = project(fields(include("geom.coordinates"), excludeId()));
        Bson unwind = unwind("$geom.coordinates");

        AggregateIterable<Document> geomCursor = treeFieldsCollection.aggregate(Arrays.asList(
                match, sort, limit, project, unwind));

        return geomCursor;
    }

    private double getCoordinate(Document doc) {
        Document dbl = (Document) doc.get("geom");
        String str = dbl.toJson();
        JsonObject jsonObject = JsonParser.parseString(str).getAsJsonObject();
        return jsonObject.get("coordinates").getAsDouble();
    }

}
