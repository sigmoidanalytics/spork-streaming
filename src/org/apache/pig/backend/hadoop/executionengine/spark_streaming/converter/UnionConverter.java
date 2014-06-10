package org.apache.pig.backend.hadoop.executionengine.spark_streaming.converter;

import java.io.IOException;
import java.util.List;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POUnion;
import org.apache.pig.backend.hadoop.executionengine.spark_streaming.SparkUtil;
import org.apache.pig.data.Tuple;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class UnionConverter implements POConverter<Tuple, Tuple, POUnion> {

    private final JavaStreamingContext sc;

    public UnionConverter(JavaStreamingContext sc) {
        this.sc = sc;
    }

   
	@Override
	public JavaDStream<Tuple> convert(List<JavaDStream<Tuple>> predecessors,
			POUnion physicalOperator) throws IOException {
		 SparkUtil.assertPredecessorSizeGreaterThan(predecessors, physicalOperator, 0);
		   JavaDStream<Tuple> unionDStream=   sc.union(predecessors.get(0), predecessors.subList(1, predecessors.size()));
	       return unionDStream;
	}

}