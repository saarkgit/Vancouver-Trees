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

    public void friendlyNameBySection(int n, int m) {
        if (n <= 0 || m <= 0) {
            System.out.println("Please enter a value greater than 0");
            return;
        }

        db.getCollection("treesInSection").drop();

        double[] corners = getBoundingCoordinates(); // { geomMinLong, geomMinLat, geomMaxLong, geomMaxLat }
        // for (double d : corners) {
        //     System.out.println(d);
        // }

        Bson match = null;
        Bson group = group("$friendly_name", Accumulators.sum("count", 1));

        List<Document> allBoxesDocs = new ArrayList<>();
        double[] sectionBox = { 0, 0, 0, 0 };
        double xIncrement = (corners[2] - corners[0]) / n;
        double yIncrement = (corners[3] - corners[1]) / m;

        for (int i = 0; i < n; i++) {
            sectionBox[0] = corners[0] + (xIncrement * i);
            sectionBox[2] = corners[0] + (xIncrement * (i + 1));
            // geoWithinBox(fieldName, lowerLeftX, lowerLeftY, upperRightX, upperRightY)
            // corners[] = { geomMinLong, geomMinLat, geomMaxLong, geomMaxLat }

            for (int j = 0; j < m; j++) {
                sectionBox[1] = corners[3] - (yIncrement * (j + 1));
                sectionBox[3] = corners[3] - (yIncrement * j);
                match = match(and(
                        exists("geom"),
                        geoWithinBox(
                                "geom.coordinates",
                                sectionBox[0],
                                sectionBox[1],
                                sectionBox[2],
                                sectionBox[3])));

                AggregateIterable<Document> sectionCursor = treeFieldsCollection.aggregate(
                        Arrays.asList(match, group));

                List<Document> FNInBoxDocs = new ArrayList<>();
                for (Document document : sectionCursor) {
                    FNInBoxDocs.add(document);
                }

                Document boxDoc = new Document();
                boxDoc.append("section_x", i);
                boxDoc.append("section_y", j);
                boxDoc.append("trees_by_section", FNInBoxDocs);
                allBoxesDocs.add(boxDoc);
            }

        }

        db.getCollection("treesInSection").insertMany(allBoxesDocs);
    }

    private double[] getBoundingCoordinates() {
        Bson sort = null;
        AggregateIterable<Document> geomCursor = null;
        Document doc = null;

        // get geomMinLong
        sort = sort(ascending("geom.coordinates.0"));
        geomCursor = boundingCoordHelper(sort);
        doc = geomCursor.first();
        double geomMinLong = getCoordinate(doc);

        // get geomMaxLong
        sort = sort(descending("geom.coordinates.0"));
        geomCursor = boundingCoordHelper(sort);
        doc = geomCursor.first();
        double geomMaxLong = getCoordinate(doc);

        // get geomMinLat
        sort = sort(ascending("geom.coordinates.1"));
        geomCursor = boundingCoordHelper(sort);

        Iterator<Document> it = geomCursor.iterator();
        while (it.hasNext()) {
            doc = it.next();
        }

        double geomMinLat = getCoordinate(doc);

        // get geomMaxLat
        sort = sort(descending("geom.coordinates.1"));
        geomCursor = boundingCoordHelper(sort);

        it = geomCursor.iterator();
        while (it.hasNext()) {
            doc = it.next();
        }

        double geomMaxLat = getCoordinate(doc);

        double[] result = { geomMinLong, geomMinLat, geomMaxLong, geomMaxLat };
        return result;
    }

    private AggregateIterable<Document> boundingCoordHelper(Bson sort) {
        // Vancouver area
        Bson match = match(and(
                exists("geom"),
                lt("geom.coordinates.0", -122.5),
                gt("geom.coordinates.0", -123.5),
                lt("geom.coordinates.1", 50),
                gt("geom.coordinates.1", 49)));
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
