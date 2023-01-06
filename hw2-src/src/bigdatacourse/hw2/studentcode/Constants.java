package bigdatacourse.hw2.studentcode;

import java.util.Arrays;
import java.util.List;

public class Constants {

    // General
    public static final String NOT_AVAILABLE_VALUE = "na";
    public static final String ITEM_DOES_NOT_EXIST = "not exists";
    public static final int    NUM_OF_THREADS      = 250;

    // Column Names
    public static final String ASIN          = "asin";
    public static final String TITLE         = "title";
    public static final String IMAGE         = "image";
    public static final String CATEGORIES    = "categories";
    public static final String DESCRIPTION   = "description";
    public static final String REVIEWER_ID   = "reviewerID";
    public static final String REVIEWER_NAME = "reviewerName";
    public static final String RATING        = "overall";
    public static final String SUMMARY       = "summary";
    public static final String REVIEW_TEXT   = "reviewText";
    public static final String TIME          = "unixReviewTime";


    // Tables Constants
    public static final String TABLE_ITEMS           = "items";
    public static final String TABLE_REVIEWS_BY_ITEM = "reviews_by_item";
    public static final String TABLE_REVIEWS_BY_USER = "reviews_by_user";

    public static final List<String> TABLE_ITEMS_KEYS           =
            Arrays.asList(ASIN, TITLE, IMAGE, CATEGORIES, DESCRIPTION);
    public static final List<String> TABLE_REVIEWS_BY_ITEM_KEYS =
            Arrays.asList(ASIN, TIME, REVIEWER_ID, REVIEWER_NAME, RATING, SUMMARY, REVIEW_TEXT);
    public static final List<String> TABLE_REVIEWS_BY_USER_KEYS =
            Arrays.asList(REVIEWER_ID, TIME, ASIN, REVIEWER_NAME, RATING, SUMMARY, REVIEW_TEXT);


    // Create Table Queries
    public static final String CREATE_TABLE_ITEMS =
            "CREATE TABLE " + TABLE_ITEMS + "(" + ASIN + " text," + TITLE + " text," + IMAGE + " text," + CATEGORIES +
            " set<text>," + DESCRIPTION + " text," + "PRIMARY KEY (" + ASIN + ")) ";

    public static final String CREATE_TABLE_REVIEWS_BY_ITEM =
            "CREATE TABLE " + TABLE_REVIEWS_BY_ITEM + "(" + ASIN + " text," + TIME + " int," + REVIEWER_ID + " text," +
            REVIEWER_NAME + " text," + RATING + " int," + SUMMARY + " text," + REVIEW_TEXT + " text," +
            "PRIMARY KEY ((" + ASIN + "), " + TIME + ", " + REVIEWER_ID + ")) WITH CLUSTERING ORDER BY (" + TIME +
            " DESC, " + REVIEWER_ID + " DESC)";

    public static final String CREATE_TABLE_REVIEWS_BY_USER =
            "CREATE TABLE " + TABLE_REVIEWS_BY_USER + "(" + REVIEWER_ID + " text," + TIME + " int," + ASIN + " text," +
            REVIEWER_NAME + " text," + RATING + " int," + SUMMARY + " text," + REVIEW_TEXT + " text," +
            "PRIMARY KEY ((" + REVIEWER_ID + "), " + TIME + ", " + ASIN + ")) WITH CLUSTERING ORDER BY (" + TIME +
            " DESC, " + ASIN + " DESC)";

    // Insert Queries
    public static final String ITEMS_INSERT =
            "INSERT INTO " + TABLE_ITEMS + "(" + ASIN + ", " + TITLE + ", " + IMAGE + ", " + CATEGORIES + ", " +
            DESCRIPTION + ") VALUES(?, ?, ?, ?, ?)";

    public static final String REVIEWS_BY_ITEM_INSERT =
            "INSERT INTO " + TABLE_REVIEWS_BY_ITEM + "(" + ASIN + ", " + TIME + ", " + REVIEWER_ID + ", " +
            REVIEWER_NAME + ", " + RATING + ", " + SUMMARY + ", " + REVIEW_TEXT + ") VALUES(?, ?, ?, ?, ?, ?, ?)";

    public static final String REVIEWS_BY_USER_INSERT =
            "INSERT INTO " + TABLE_REVIEWS_BY_USER + "(" + REVIEWER_ID + ", " + TIME + ", " + ASIN + ", " +
            REVIEWER_NAME + ", " + RATING + ", " + SUMMARY + ", " + REVIEW_TEXT + ") VALUES(?, ?, ?, ?, ?, ?, ?)";


    // Select Queries
    public static final String ITEMS_SELECT           = "SELECT * FROM " + TABLE_ITEMS + " WHERE " + ASIN + " = ?";
    public static final String REVIEWS_BY_ITEM_SELECT =
            "SELECT * FROM " + TABLE_REVIEWS_BY_ITEM + " WHERE " + ASIN + " = ?";
    public static final String REVIEWS_BY_USER_SELECT =
            "SELECT * FROM " + TABLE_REVIEWS_BY_USER + " WHERE " + REVIEWER_ID + " = ?";


}
