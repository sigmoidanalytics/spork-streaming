package org.apache.pig.backend.hadoop.executionengine.spark_streaming.converter;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.data.Tuple;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.dstream.DStream;
import org.apache.spark.streaming.api.java.JavaDStream;

import scala.Tuple2;

/**
 * Given an RDD and a PhysicalOperater, and implementation of this class can convert the RRD to
 * another RRD.
 *
 * @author billg
 */
public interface POConverter<IN, OUT, T extends PhysicalOperator> {
    JavaDStream<Tuple> convert(List<JavaDStream<IN>> inputDStream, T physicalOperator) throws IOException;
	
}
