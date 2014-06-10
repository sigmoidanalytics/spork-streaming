package org.apache.pig.backend.hadoop.executionengine.spark_streaming.converter;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PODistinct;
import org.apache.pig.backend.hadoop.executionengine.spark_streaming.SparkUtil;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;

import scala.Function1;
import scala.Tuple2;
import scala.runtime.AbstractFunction1;
import org.apache.spark.streaming.api.java.JavaDStream;

@SuppressWarnings({ "serial"})
public  class  DistinctConverter implements POConverter<Tuple, Tuple, PODistinct> {
    private static final Log LOG = LogFactory.getLog(DistinctConverter.class);
    private static final Function1<Tuple2<Tuple,Object>, Tuple> TO_VALUE_FUNCTION = new ToValueFunction();
    
    @Override
	public JavaDStream<Tuple> convert(List<JavaDStream<Tuple>> predecessors,
			PODistinct poDistinct) throws IOException {
        SparkUtil.assertPredecessorSize(predecessors, poDistinct, 1);
        JavaDStream<Tuple> inputDStream = predecessors.get(0);
        LOG.info("Doing Distinct on predecessors "+predecessors.size());
        return new JavaDStream<Tuple>(inputDStream.dstream().countByValue(1).map(TO_VALUE_FUNCTION , SparkUtil.getManifest(Tuple.class)),SparkUtil.getManifest(Tuple.class));
    }
    public static final class ToValueFunction extends AbstractFunction1<Tuple2<Tuple, Object>, Tuple> implements Serializable {
        @Override
        public Tuple apply(Tuple2<Tuple, Object> input) {
        	DefaultTuple var=new DefaultTuple();
			var.append(input._1);
        	return var;
        }
    }
}