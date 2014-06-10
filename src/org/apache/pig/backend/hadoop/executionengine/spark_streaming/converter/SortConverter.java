package org.apache.pig.backend.hadoop.executionengine.spark_streaming.converter;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PODistinct;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSort;
import org.apache.pig.backend.hadoop.executionengine.spark_streaming.SparkUtil;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;

import scala.Function1;
import scala.Function2;
import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.PairRDDFunctions;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.dstream.DStream;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLimit;
import org.apache.pig.data.Tuple;

import scala.collection.Iterator;
import scala.collection.JavaConversions;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;

@SuppressWarnings("serial")
public class SortConverter implements POConverter<Tuple, Tuple, POSort> {
    private static final Log LOG = LogFactory.getLog(SortConverter.class);

    @Override
	public JavaDStream<Tuple> convert(List<JavaDStream<Tuple>> predecessors,
			final POSort sortOperator) throws IOException {		
        SparkUtil.assertPredecessorSize(predecessors, sortOperator, 1);
        JavaDStream<Tuple> rdd = predecessors.get(0);
        DStream<Tuple2<Tuple, Object>> rddPair =
                rdd.dstream().map(new ToKeyValueFunction(),
                        SparkUtil.<Tuple, Object>getTuple2Manifest());

        JavaPairDStream<Tuple, Object> r = new JavaPairDStream<Tuple, Object>(rddPair, SparkUtil.getManifest(Tuple.class),
                                                        SparkUtil.getManifest(Object.class));

        JavaPairDStream<Tuple, Object> sorted = r.transform(
        	     new Function<JavaPairRDD<Tuple, Object>, JavaPairRDD<Tuple, Object>>() {
        	         public JavaPairRDD<Tuple, Object> call(JavaPairRDD<Tuple, Object> in) throws Exception {
        	           return in.sortByKey(sortOperator.getmComparator(),false);
        	         }
        	       });
        JavaDStream<Tuple> mapped = new JavaDStream<Tuple>(sorted.dstream().map(new ToValueFunction(),SparkUtil.getManifest(Tuple.class)), SparkUtil.getManifest(Tuple.class));

        return mapped;
    }

    private static class ToValueFunction extends AbstractFunction1<Tuple2<Tuple, Object>,Tuple> implements Serializable {

        @Override
        public Tuple apply(Tuple2<Tuple, Object> t) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Sort ToKeyValueFunction in "+t);
            }
            Tuple key = t._1;
            return key;
        }
    }

    private static class ToKeyValueFunction extends AbstractFunction1<Tuple,Tuple2<Tuple, Object>> implements Serializable {

        @Override
        public Tuple2<Tuple, Object> apply(Tuple t) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Sort ToKeyValueFunction in "+t);
            }
            Tuple key = t;
            Object value = null;
            // (key, value)
            Tuple2<Tuple, Object> out = new Tuple2<Tuple, Object>(key, value);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Sort ToKeyValueFunction out "+out);
            }
            return out;
        }
    }

	

}
	