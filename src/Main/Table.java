package Main;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Hashtable;

public class Table {

	String strTableName;
	String strClusteringKeyColumn;
	Hashtable<String, String> htblColNameType;

	Table(String strTableName, String strClusteringKeyColumn, Hashtable<String, String> htblColNameType)
			throws IOException, DBAppException {
		this.strTableName = strTableName;
		this.strClusteringKeyColumn = strClusteringKeyColumn;
		this.htblColNameType = htblColNameType;
		if(!htblColNameType.containsKey(strClusteringKeyColumn))
			throw new DBAppException("Clustering Key is not defined in the Table");
		createMetaData();
		
	}

	void createMetaData() throws IOException {
		File dir = new File("data/metadata.csv");
		FileWriter fileWriter = new FileWriter(dir, true);

		BufferedWriter bw = new BufferedWriter(fileWriter);
		PrintWriter out = new PrintWriter(bw);
		for (String colName : htblColNameType.keySet()) {
			String dataType = htblColNameType.get(colName);
			out.print(strTableName + ", " + colName + ", " + dataType + ", "
					+ (colName.equals(strClusteringKeyColumn) ? "True, " : "False, ") + "False\n");
		}
		out.flush();
		out.close();
		fileWriter.close();
	}
}
