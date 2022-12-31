package bigdatacourse.hw2.studentcode;

import java.nio.file.Paths;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import com.datastax.oss.driver.api.core.CqlSession;

import bigdatacourse.hw2.HW2API;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import org.json.JSONException;
import org.json.JSONObject;

public class HW2StudentAnswer implements HW2API {

    // general consts
    public static final String NOT_AVAILABLE_VALUE = "na";
    public static final String ITEM_DOES_NOT_EXIST = "not exists";
    public static final int    NUM_OF_THREADS      = 100; // TODO - change to 250?

    public static final String PATH_TO_ITEMS   = "/Users/tamiry/Desktop/bigDataHW2/data/meta_Office_Products.json";
    // TODO - delete when done
    public static final String PATH_TO_REVIEWS = "/Users/tamiry/Desktop/bigDataHW2/data/reviews_Office_Products.json";
    // TODO - delet when done

    // CQL stuff
    public static final String       TABLE_ITEMS      = "items";
    public static final List<String> TABLE_ITEMS_KEYS =
            Arrays.asList("asin", "title", "image", "categories", "description");

    public static final String TABLE_USER_REVIEWS = "user_reviews";
    public static final String TABLE_ITEM_REVIEWS = "item_reviews";


    public static final String CQL_ITEMS_INSERT =
            "INSERT INTO " + TABLE_ITEMS + "(asin, title, image, categories, description) VALUES(?, ?, ?, ?, ?)";

    public static final String CQL_ITEMS_SELECT = "SELECT * FROM " + TABLE_ITEMS + " WHERE asin = ?";


    public static final String CQL_CREATE_TABLE_ITEMS =
            "CREATE TABLE " + TABLE_ITEMS + "(" + "asin text," + "title text," + "image text," + "categories text," +
            "description text," + "PRIMARY KEY (asin)" + ") ";

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
        session.execute(CQL_CREATE_TABLE_ITEMS);
        System.out.println("created table: " + TABLE_ITEMS);
    }

    @Override
    public void initialize() {
        pstmtAddToItems = session.prepare(CQL_ITEMS_INSERT);
        pstmtSelectFromItems = session.prepare(CQL_ITEMS_SELECT);
    }

    @Override
    public void loadItems(String pathItemsFile) throws Exception {
        BufferedReader reader;

        try {
            reader = new BufferedReader(new FileReader(pathItemsFile));
            String     itemString = reader.readLine();
            JSONObject itemJsonObject;

            while (itemString != null) {
                itemJsonObject = new JSONObject(itemString);
                fillItemsJson(itemJsonObject);
                BoundStatement bstmtAddToItems = pstmtAddToItems.bind().setString(0, itemJsonObject.getString("asin"))
                                                                .setString(1, itemJsonObject.getString("title"))
                                                                .setString(2, itemJsonObject.getString("image"))
                                                                .setString(3, itemJsonObject.getString("categories"))
                                                                .setString(4, itemJsonObject.getString("description"));
                session.execute(bstmtAddToItems);

                // read next line
                itemString = reader.readLine();
                break; // in order to read only first line since this is a test
            }

            reader.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void loadReviews(String pathReviewsFile) throws Exception {
        //TODO: implement this function
        System.out.println("TODO: implement this function...");
    }

    @Override
    public void item(String asin) {
        //TODO: implement this function
        System.out.println("TODO: implement this function...");

        // required format - example for asin B005QB09TU
        System.out.println("asin: " + "B005QB09TU");
        System.out.println("title: " + "Circa Action Method Notebook");
        System.out.println("image: " + "http://ecx.images-amazon.com/images/I/41ZxT4Opx3L._SY300_.jpg");
        System.out.println("categories: " + new HashSet<String>(
                Arrays.asList("Notebooks & Writing Pads", "Office & School Supplies", "Office Products", "Paper")));
        System.out.println("description: " +
                           "Circa + Behance = Productivity. The minute-to-minute flexibility of Circa note-taking meets the organizational power of the Action Method by Behance. The result is enhanced productivity, so you'll formulate strategies and achieve objectives even more efficiently with this Circa notebook and project planner. Read Steve's blog on the Behance/Levenger partnership Customize with your logo. Corporate pricing available. Please call 800-357-9991.");
        ;

        // required format - if the asin does not exists return this value
        System.out.println("not exists");
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
        for (String key : TABLE_ITEMS_KEYS) {
            try {
                Object value = jsonObject.get(key);
                jsonObject.put(key, value.toString());
            }
            catch (JSONException e) {
                jsonObject.put(key, NOT_AVAILABLE_VALUE);
            }
        }
    }


    public static void main(String[] args) {
        //        BufferedReader reader;
        //
        //        try {
        //            reader = new BufferedReader(new FileReader(PATH_TO_ITEMS));
        //            String     itemString = reader.readLine();
        //            JSONObject itemJsonObject;
        //
        //            while (itemString != null) {
        //                itemJsonObject = new JSONObject(itemString);
        //                fillJsonIfNull(itemJsonObject);
        //                System.out.println(itemString);
        //                System.out.println(itemJsonObject);
        //                System.out.println(itemJsonObject.getString("asin"));
        //                // read next line
        //                itemString = reader.readLine();
        //                break; // in order to read only first line since this is a test
        //            }
        //
        //            reader.close();
        //        }
        //        catch (IOException e) {
        //            e.printStackTrace();
        //        }
    }


}
