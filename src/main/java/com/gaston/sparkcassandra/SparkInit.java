package com.gaston.sparkcassandra;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;
import com.gaston.sparkcassandra.model.Entry;

import scala.Tuple2;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;

public class SparkInit {
	private SparkConf conf;
	private JavaRDD<Entry> rdd_to_show;
	
	private SparkInit(SparkConf conf){
		this.conf = conf;
	}
	
	private void run(){
		JavaSparkContext sc = new JavaSparkContext(conf);
		generateData(sc);
		compute(sc);
		showResults(sc);
		sc.stop();
	}
	
	private void generateData(JavaSparkContext sc){
		//CassandraInit.init(sc);
		System.out.println("Cassandra keyspace and table generated");
		List<Entry> entries = new ArrayList<>(1240901);
		for (int i = 0; i < 1240901; i++) {
			entries.add(new Entry(Long.toString(System.nanoTime()), "Info is " + new Random().nextLong() + " another " + UUID.randomUUID(), new Random().nextInt(2)));
		}
		System.out.println("Data generated in memory");
		JavaRDD<Entry> entryRDD = sc.parallelize(entries);
		javaFunctions(entryRDD).writerBuilder("log", "entry", mapToRow(Entry.class)).saveToCassandra();
		System.out.println("Data stored in cassandra");
	}
	
	private void compute(JavaSparkContext sc){
		CassandraJavaRDD<Entry> rdd = javaFunctions(sc).cassandraTable("log", "entry", mapRowTo(Entry.class));
		System.out.println("Count Total: " + rdd.count());
		JavaRDD<Entry> filter_rdd = rdd.filter(e -> e.getInfo().contains("abcd")).cache();
		System.out.println("Count Filter: " + filter_rdd.count());
		System.out.println("Count Filter Cached: " + filter_rdd.count());
		JavaRDD<Entry> filter_source1_rdd = filter_rdd.filter(e -> e.getSource() == 1);
		System.out.println("Count Filter Source 1 Cached: " + filter_source1_rdd.count());
		rdd_to_show = filter_source1_rdd;
	}
	
	private void showResults(JavaSparkContext sc){
		rdd_to_show.map(e -> new Tuple2<String, String>(e.getDate(), e.getInfo()))
			.collect()
			.forEach(t -> System.out.println("Date: " + t._1 + " | Info: " + t._2));
	}
	
	public static void main(String[] args){
		SparkConf conf = new SparkConf();
		conf.setAppName("Spark Cassandra demo");
		conf.setMaster("local[2]");
		conf.set("spark.cassandra.connection.host", "127.0.0.1");
		SparkInit app = new SparkInit(conf);
		app.run();
	
	}
	
}
