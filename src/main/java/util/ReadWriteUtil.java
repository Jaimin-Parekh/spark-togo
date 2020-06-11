package util;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class ReadWriteUtil {

	public static void write(Dataset<Row> ds, String driver, Map<Object, Object> options){
		ds.write().format(driver);
		
	}
}
