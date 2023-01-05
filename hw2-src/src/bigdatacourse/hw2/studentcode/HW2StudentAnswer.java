package bigdatacourse.hw2.studentcode;

import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.datastax.oss.driver.api.core.CqlSession;

import bigdatacourse.hw2.HW2API;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class HW2StudentAnswer implements HW2API {

    // cassandra session
    private CqlSession session;


    // prepared statements
    private PreparedStatement pstmtAddToItems;
    private PreparedStatement pstmtSelectFromItems;


    @Override
    public void connect(String pathAstraDBBundleFile, String username, String password, String keyspace) {
        if (session != null) {
            System.out.println("ERROR - cassandra is already connected");
            return;
        }

        System.out.println("Initializing connection to Cassandra...");

        this.session = CqlSession.builder().withCloudSecureConnectBundle(Paths.get(pathAstraDBBundleFile))
                                 .withAuthCredentials(username, password).withKeyspace(keyspace).build();

        System.out.println("Initializing connection to Cassandra... Done");
    }


    @Override
    public void close() {
        if (session == null) {
            System.out.println("Cassandra connection is already closed");
            return;
        }

        System.out.println("Closing Cassandra connection...");
        session.close();
        System.out.println("Closing Cassandra connection... Done");
    }


    @Override
    public void createTables() {
        session.execute(Constants.CREATE_TABLE_ITEMS);
        System.out.println("created table: " + Constants.TABLE_ITEMS);
    }

    @Override
    public void initialize() {
        pstmtAddToItems = session.prepare(Constants.ITEMS_INSERT);
        pstmtSelectFromItems = session.prepare(Constants.ITEMS_SELECT);
    }

    @Override
    public void loadItems(String pathItemsFile) throws Exception {
        BufferedReader reader;
        reader = new BufferedReader(new FileReader(pathItemsFile));
        int count = 0;

        String     itemString = reader.readLine();
        JSONObject itemJsonObject;

        ExecutorService executor = Executors.newFixedThreadPool(Constants.NUM_OF_THREADS);

        while (itemString != null) {
            itemJsonObject = new JSONObject(itemString);
            fillItemsJson(itemJsonObject);

            JSONObject finalItemJsonObject = itemJsonObject;
            executor.execute(() -> {
                insertToItems(session, pstmtAddToItems, finalItemJsonObject);
            });
            count++;
            System.out.println(count);

            itemString = reader.readLine();
        }

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.SECONDS);

        reader.close();
    }

    @Override
    public void loadReviews(String pathReviewsFile) throws Exception {
        //TODO: implement this function
        System.out.println("TODO: implement this function...");
    }

    @Override
    public void item(String asin) {
        BoundStatement bstmtSelectFromItems = pstmtSelectFromItems.bind().setString(0, asin);
        ResultSet      rs                   = session.execute(bstmtSelectFromItems);
        Row            row                  = rs.one();
        if (row != null) {
            while (row != null) {
                System.out.println("asin: " + row.getString(Constants.ASIN));
                System.out.println("title: " + row.getString(Constants.TITLE));
                System.out.println("image: " + row.getString(Constants.IMAGE));
                System.out.println("categories: " + row.getSet(Constants.CATEGORIES, String.class));
                System.out.println("description: " + row.getString(Constants.DESCRIPTION));

                row = rs.one();
            }
        }
        else {
            System.out.println(Constants.ITEM_DOES_NOT_EXIST);
        }
    }


    @Override
    public void userReviews(String reviewerID) {
        //TODO: implement this function
        System.out.println("TODO: implement this function...");


        // required format - example for reviewerID A17OJCRPMYWXWV
        System.out.println("time: " + Instant.ofEpochSecond(1362614400) + ", asin: " + "B005QDG2AI" + ", reviewerID: " +
                           "A17OJCRPMYWXWV" + ", reviewerName: " + "Old Flour Child" + ", rating: " + 5 +
                           ", summary: " + "excellent quality" + ", reviewText: " +
                           "These cartridges are excellent .  I purchased them for the office where I work and they perform  like a dream.  They are a fraction of the price of the brand name cartridges.  I will order them again!");

        System.out.println("time: " + Instant.ofEpochSecond(1360108800) + ", asin: " + "B003I89O6W" + ", reviewerID: " +
                           "A17OJCRPMYWXWV" + ", reviewerName: " + "Old Flour Child" + ", rating: " + 5 +
                           ", summary: " + "Checkbook Cover" + ", reviewText: " +
                           "Purchased this for the owner of a small automotive repair business I work for.  The old one was being held together with duct tape.  When I saw this one on Amazon (where I look for almost everything first) and looked at the price, I knew this was the one.  Really nice and very sturdy.");

        System.out.println("total reviews: " + 2);
    }

    @Override
    public void itemReviews(String asin) {
        //TODO: implement this function
        System.out.println("TODO: implement this function...");


        // required format - example for asin B005QDQXGQ
        System.out.println("time: " + Instant.ofEpochSecond(1391299200) + ", asin: " + "B005QDQXGQ" + ", reviewerID: " +
                           "A1I5J5RUJ5JB4B" + ", reviewerName: " + "T. Taylor \"jediwife3\"" + ", rating: " + 5 +
                           ", summary: " + "Play and Learn" + ", reviewText: " +
                           "The kids had a great time doing hot potato and then having to answer a question if they got stuck with the &#34;potato&#34;. The younger kids all just sat around turnin it to read it.");

        System.out.println("time: " + Instant.ofEpochSecond(1390694400) + ", asin: " + "B005QDQXGQ" + ", reviewerID: " +
                           "AF2CSZ8IP8IPU" + ", reviewerName: " + "Corey Valentine \"sue\"" + ", rating: " + 1 +
                           ", summary: " + "Not good" + ", reviewText: " +
                           "This Was not worth 8 dollars would not recommend to others to buy for kids at that price do not buy");

        System.out.println("time: " + Instant.ofEpochSecond(1388275200) + ", asin: " + "B005QDQXGQ" + ", reviewerID: " +
                           "A27W10NHSXI625" + ", reviewerName: " + "Beth" + ", rating: " + 2 + ", summary: " +
                           "Way overpriced for a beach ball" + ", reviewText: " +
                           "It was my own fault, I guess, for not thoroughly reading the description, but this is just a blow-up beach ball.  For that, I think it was very overpriced.  I thought at least I was getting one of those pre-inflated kickball-type balls that you find in the giant bins in the chain stores.  This did have a page of instructions for a few different games kids can play.  Still, I think kids know what to do when handed a ball, and there's a lot less you can do with a beach ball than a regular kickball, anyway.");

        System.out.println("total reviews: " + 3);
    }

    private static void fillItemsJson(JSONObject jsonObject) {
        for (String key : Constants.TABLE_ITEMS_KEYS) {

            if (key.equals("categories")) {
                try {
                    jsonObject.get(key);
                }
                catch (JSONException e) {
                    jsonObject.put(key, new HashSet<>());
                }
            }
            else {
                try {
                    jsonObject.get(key);
                }
                catch (JSONException e) {
                    jsonObject.put(key, Constants.NOT_AVAILABLE_VALUE);
                }
            }
        }
    }

    private static Set<String> jsonArrayToStringSet(JSONArray jsonArray) {
        Set<String> retVal = new HashSet<>();

        if (jsonArray != null) {

            for (int i = 0; i < jsonArray.length(); i++) {
                retVal.add(jsonArray.get(i).toString());
            }
        }

        return retVal;
    }

    public static void insertToItems(CqlSession session, PreparedStatement pstmt, JSONObject jsonObject) {

        BoundStatement bstmtAddToItems = pstmt.bind().setString(Constants.ASIN, jsonObject.getString(Constants.ASIN))
                                              .setString(Constants.TITLE, jsonObject.getString(Constants.TITLE))
                                              .setString(Constants.IMAGE, jsonObject.getString(Constants.IMAGE))
                                              .setSet(Constants.CATEGORIES, jsonArrayToStringSet(
                                                              jsonObject.getJSONArray(Constants.CATEGORIES).getJSONArray(0)),
                                                      String.class).setString(Constants.DESCRIPTION,
                                                                              jsonObject.getString(
                                                                                      Constants.DESCRIPTION));

        session.execute(bstmtAddToItems);
    }

}
