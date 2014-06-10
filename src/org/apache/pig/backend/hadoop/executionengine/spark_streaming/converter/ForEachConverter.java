package org.apache.pig.backend.hadoop.executionengine.spark_streaming.converter;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.spark_streaming.SparkUtil;
import org.apache.pig.data.Tuple;

import scala.collection.Iterator;
import scala.collection.JavaConversions;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.api.java.JavaDStream;

/**
 * Convert that is able to convert an RRD to another RRD using a POForEach
 * @author billg
 */
@SuppressWarnings({ "serial"})
public class ForEachConverter implements POConverter<Tuple, Tuple, POForEach> {

    @Override
    public JavaDStream<Tuple> convert(List<JavaDStream<Tuple>> predecessors,
			POForEach physicalOperator) throws IOException {
        SparkUtil.assertPredecessorSize(predecessors, physicalOperator, 1);
        JavaDStream<Tuple> rdd = predecessors.get(0);
        ForEachFunction forEachFunction = new ForEachFunction(physicalOperator);
        return new JavaDStream<Tuple>(rdd.dstream().mapPartitions(forEachFunction, true, SparkUtil.getManifest(Tuple.class)),
        		SparkUtil.getManifest(Tuple.class));
    }

    private static class ForEachFunction extends Function<Iterator<Tuple>, Iterator<Tuple>>
            implements Serializable {

        private POForEach poForEach;

        private ForEachFunction(POForEach poForEach) {
            this.poForEach = poForEach;
        }

        public Iterator<Tuple> call(Iterator<Tuple> i) {
            final java.util.Iterator<Tuple> input = JavaConversions.asJavaIterator(i);
            Iterator<Tuple> output = JavaConversions.asScalaIterator(new POOutputConsumerIterator(input) {
                protected void attach(Tuple tuple) {
                    poForEach.setInputs(null);
                    poForEach.attachInput(tuple);
                }

                protected Result getNextResult() throws ExecException {
                    return poForEach.getNextTuple();
                }
            });
            return output;
        }
    }


}