package util;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class CassandraUtil {
	public static void write(Dataset<Row> ds, String keyspace, String table) {
		ds.write().format("org.apache.spark.sql.cassandra")
		.option("keyspace", keyspace)
		.option("table", table)
		.mode(SaveMode.Append)
		.save();
	}
	
	public static Dataset<Row> read(SparkSession session, String keyspace, String table, String condition) {
		return session.read().format("org.apache.spark.sql.cassandra")
		.option("keyspace", keyspace)
		.option("table", table).load().filter(condition);
	}
}
