package org.apache.pig.backend.hadoop.executionengine.spark_streaming.converter;

import java.io.IOException;
import java.util.List;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSplit;
import org.apache.pig.backend.hadoop.executionengine.spark_streaming.SparkUtil;
import org.apache.pig.data.Tuple;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.api.java.JavaDStream;

public class SplitConverter implements POConverter<Tuple, Tuple, POSplit> {
	//Does not split right now
    @Override
    public JavaDStream<Tuple> convert(List<JavaDStream<Tuple>> predecessors,
			POSplit poSplit) throws IOException {
	    SparkUtil.assertPredecessorSize(predecessors, poSplit, 1);
        return predecessors.get(0);
    }

	
}