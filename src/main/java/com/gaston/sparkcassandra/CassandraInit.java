package com.gaston.sparkcassandra;

import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;

public class CassandraInit {

	public static void init(JavaSparkContext sc){
		CassandraConnector connector = CassandraConnector.apply(sc.getConf());
		try(Session session = connector.openSession()){
			session.execute("DROP KEYSPACE IF EXISTS log");
			session.execute("CREATE KEYSPACE log WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
		    session.execute("CREATE TABLE log.entry ( date text, info text, source int, PRIMARY KEY (date))");
		  }
	}
	
}
