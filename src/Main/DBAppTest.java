package Main;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;
import java.util.Hashtable;
import java.util.Properties;

public class DBAppTest {

	static void create() throws DBAppException, IOException {
		Hashtable htblColNameType = new Hashtable();
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("gpa", "java.lang.Double");
		htblColNameType.put("date", "java.lang.Date");
		DBApp.createTable("Student", "id", htblColNameType);
	}
	static void insert() throws ClassNotFoundException, DBAppException, IOException, ParseException {
		Hashtable htblColNameValue = new Hashtable();
		htblColNameValue.put("id", 4);
		htblColNameValue.put("name", new String("Zoz"));
		htblColNameValue.put("gpa", 1.9);
		DBApp.insertIntoTable("Student", htblColNameValue);
	}
	static void delete() throws FileNotFoundException, ParseException, IOException {
		Hashtable htblColNameValue = new Hashtable();
		htblColNameValue.put("gpa", 1.9);
		DBApp.deleteFromTable("Student", htblColNameValue);
	}
	public static void main(String[] args) throws DBAppException, IOException, ClassNotFoundException, ParseException {
		Properties properties = new Properties();
		FileInputStream fis = new FileInputStream("config/DBApp.properties");
		properties.load(fis);
		DBApp.maxTuplesPerPage = Integer.parseInt(properties.getProperty("MaximumRowsCountinPage"));
		Bitmap.maxTuplesPerIndex = Integer.parseInt(properties.getProperty("BitmapSize"));
		String strTableName = "Student";
		create();
		DBApp.createBitmapIndex(strTableName, "gpa");
		insert();
//		delete();
		DBApp.readTables(strTableName);
		Bitmap.readBitmap(strTableName, "gpa");
		
	}

}
