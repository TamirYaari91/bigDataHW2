package bigdatacourse.hw2.studentcode;

import java.util.Arrays;
import java.util.List;

public class Constants {

    // General
    public static final String NOT_AVAILABLE_VALUE = "na";
    public static final String ITEM_DOES_NOT_EXIST = "not exists";
    public static final int    NUM_OF_THREADS      = 250; // TODO - change to 250?

    // Column Names
    public static final String ASIN        = "asin";
    public static final String TITLE       = "title";
    public static final String IMAGE       = "image";
    public static final String CATEGORIES  = "categories";
    public static final String DESCRIPTION = "description";


    // Tables Constants
    public static final String TABLE_ITEMS           = "items";
    public static final String TABLE_REVIEWS_BY_ITEM = "reviews_by_item";
    public static final String TABLE_REVIEWS_BY_USER = "reviews_by_user";

    public static final List<String> TABLE_ITEMS_KEYS = Arrays.asList(ASIN, TITLE, IMAGE, CATEGORIES, DESCRIPTION);


    // Create Table Queries
    public static final String CREATE_TABLE_ITEMS =
            "CREATE TABLE " + TABLE_ITEMS + "(" + "asin text," + "title text," + "image text," +
            "categories set<text>," + "description text," + "PRIMARY KEY (asin)" + ") ";


    // Insert Queries
    public static final String ITEMS_INSERT =
            "INSERT INTO " + TABLE_ITEMS + "(asin, title, image, categories, description) VALUES(?, ?, ?, ?, ?)";


    // Select Queries
    public static final String ITEMS_SELECT = "SELECT * FROM " + TABLE_ITEMS + " WHERE asin = ?";



}
