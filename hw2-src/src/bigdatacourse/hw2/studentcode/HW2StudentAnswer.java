package bigdatacourse.hw2.studentcode;

import java.nio.file.Paths;
import java.time.Duration;
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
    private PreparedStatement pstmtAddToReviewsByItem;
    private PreparedStatement pstmtSelectFromReviewsByItem;
    private PreparedStatement pstmtAddToReviewsByUser;
    private PreparedStatement pstmtSelectFromReviewsByUser;


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

        session.execute(Constants.CREATE_TABLE_REVIEWS_BY_ITEM);
        System.out.println("created table: " + Constants.TABLE_REVIEWS_BY_ITEM);

        session.execute(Constants.CREATE_TABLE_REVIEWS_BY_USER);
        System.out.println("created table: " + Constants.TABLE_REVIEWS_BY_USER);

    }

    @Override
    public void initialize() {
        pstmtAddToItems = session.prepare(Constants.ITEMS_INSERT);
        pstmtSelectFromItems = session.prepare(Constants.ITEMS_SELECT);

        pstmtAddToReviewsByItem = session.prepare(Constants.REVIEWS_BY_ITEM_INSERT);
        pstmtSelectFromReviewsByItem = session.prepare(Constants.REVIEWS_BY_ITEM_SELECT);

        pstmtAddToReviewsByUser = session.prepare(Constants.REVIEWS_BY_USER_INSERT);
        pstmtSelectFromReviewsByUser = session.prepare(Constants.REVIEWS_BY_USER_SELECT);

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
        BufferedReader reader;
        reader = new BufferedReader(new FileReader(pathReviewsFile));
        int count = 0;

        String     reviewString = reader.readLine();
        JSONObject reviewJsonObject;

        ExecutorService executor = Executors.newFixedThreadPool(Constants.NUM_OF_THREADS);

        while (reviewString != null) {
            reviewJsonObject = new JSONObject(reviewString);
            fillReviewJson(reviewJsonObject);

            JSONObject finalReviewJsonObject = reviewJsonObject;
            executor.execute(() -> {
                insertToReviews(session, pstmtAddToReviewsByItem, finalReviewJsonObject);
                insertToReviews(session, pstmtAddToReviewsByUser, finalReviewJsonObject);
            });
            count++;
            System.out.println(count);

            reviewString = reader.readLine();
        }

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.SECONDS);

        reader.close();
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
        BoundStatement bstmtSelectFromReviewsByUser = pstmtSelectFromReviewsByUser.bind().setString(0, reviewerID);
        ResultSet      rs                           = session.execute(bstmtSelectFromReviewsByUser);
        handleReviewsQuery(rs);
    }

    @Override
    public void itemReviews(String asin) {
        BoundStatement bstmtSelectFromReviewsByItem = pstmtSelectFromReviewsByItem.bind().setString(0, asin);
        ResultSet      rs                           = session.execute(bstmtSelectFromReviewsByItem);
        handleReviewsQuery(rs);
    }

    private void handleReviewsQuery(ResultSet rs) {
        Row row          = rs.one();
        int numOfReviews = 0;
        if (row != null) {
            while (row != null) {
                System.out.println("time: " + Instant.ofEpochSecond(row.getInt(Constants.TIME)) + ", " + "asin: " +
                                   row.getString(Constants.ASIN) + ", " + "reviewerID: " +
                                   row.getString(Constants.REVIEWER_ID) + ", " + "reviewerName: " +
                                   row.getString(Constants.REVIEWER_NAME) + ", " + "rating: " +
                                   row.getInt(Constants.RATING) + ", " + "summary: " +
                                   row.getString(Constants.SUMMARY) + ", " + "reviewText: " +
                                   row.getString(Constants.REVIEW_TEXT));
                row = rs.one();
                numOfReviews++;
            }
        }
        else {
            System.out.println(Constants.ITEM_DOES_NOT_EXIST);
        }
        System.out.println("total reviews: " + numOfReviews);
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

    private static void fillReviewJson(JSONObject jsonObject) {
        for (String key : Constants.TABLES_REVIEWS_KEYS) {
            try {
                jsonObject.get(key);
            }
            catch (JSONException e) {
                jsonObject.put(key, Constants.NOT_AVAILABLE_VALUE);
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

    public static void insertToReviews(CqlSession session, PreparedStatement pstmt, JSONObject jsonObject) {

        BoundStatement bstmtAddToReviewsByUser = pstmt.bind().setInt(Constants.TIME, jsonObject.getInt(Constants.TIME))
                                                      .setString(Constants.ASIN, jsonObject.getString(Constants.ASIN))
                                                      .setString(Constants.REVIEWER_ID,
                                                                 jsonObject.getString(Constants.REVIEWER_ID))
                                                      .setString(Constants.REVIEWER_NAME,
                                                                 jsonObject.getString(Constants.REVIEWER_NAME))
                                                      .setInt(Constants.RATING, jsonObject.getInt(Constants.RATING))
                                                      .setString(Constants.SUMMARY,
                                                                 jsonObject.getString(Constants.SUMMARY))
                                                      .setString(Constants.REVIEW_TEXT,
                                                                 jsonObject.getString(Constants.REVIEW_TEXT))
                                                      .setTimeout(Duration.ofSeconds(30));

        session.execute(bstmtAddToReviewsByUser);
    }
}
