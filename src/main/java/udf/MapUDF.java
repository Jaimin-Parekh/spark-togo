package udf;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

/**
 * Convert various columns to map data type with column name as Key and
 * corresponding column value as map Value. Currently it is implemented for
 * static sequence of columns, it can be changed to set of columns, by
 * broadcasting those column names
 *
 */
public class MapUDF implements UDF1<Row, Map<String, Integer>> {

	private static final int FROM_COL = 5;
	private static final int TO_COL = 10;

	/*
	 * private static Broadcast<Set<String>> col_set;
	 * 
	 * public MapUDF(Broadcast<Set<String>> col_set) { this.col_set = col_set; }
	 */

	public Map<String, Integer> call(Row row) {
		Map<String, Integer> map = new HashMap();
		StructField[] array = row.schema().fields();
		for (int i = FROM_COL; i < TO_COL; i++) {
			if (row.getInt(i) > 0) {
				map.put(array[i].name(), row.getInt(i));
			}
		}
		return map;
	}

	public static void register(SparkSession session) {
		session.udf().register("MapUDF", new MapUDF(),
				DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType));
	}
}
