package org.apache.pig.backend.hadoop.executionengine.spark_streaming.converter;

import java.io.Serializable;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFilter;
import org.apache.pig.backend.hadoop.executionengine.spark_streaming.SparkUtil;
import org.apache.pig.data.Tuple;

import scala.runtime.AbstractFunction1;

import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.api.java.JavaDStream;

/**
 * Converter that converts an RDD to a filtered RRD using POFilter
 * @author billg
 */
@SuppressWarnings({ "serial"})
public class FilterConverter implements POConverter<Tuple, Tuple, POFilter> {

    @Override
    public JavaDStream<Tuple> convert(List<JavaDStream<Tuple>> predecessors, POFilter physicalOperator) {
        SparkUtil.assertPredecessorSize(predecessors, physicalOperator, 1);
        JavaDStream<Tuple> rdd = predecessors.get(0);
        FilterFunction filterFunction = new FilterFunction(physicalOperator);
        return new JavaDStream<Tuple>(rdd.dstream().filter(filterFunction),SparkUtil.getManifest(Tuple.class));
    }

    private static class FilterFunction extends AbstractFunction1<Tuple, Object>
            implements Serializable {

        private POFilter poFilter;

        private FilterFunction(POFilter poFilter) {
            this.poFilter = poFilter;
        }

        @Override
        public Boolean apply(Tuple v1) {
            Result result;
            try {
                poFilter.setInputs(null);
                poFilter.attachInput(v1);
                result = poFilter.getNextTuple();
            } catch (ExecException e) {
                throw new RuntimeException("Couldn't filter tuple", e);
            }

            if (result == null) { return false; }

            switch (result.returnStatus) {
                case POStatus.STATUS_OK:
                    return true;
                case POStatus.STATUS_EOP: // TODO: probably also ok for EOS, END_OF_BATCH
                    return false;
                default:
                    throw new RuntimeException("Unexpected response code from filter: " + result);
            }
        }
    }
}