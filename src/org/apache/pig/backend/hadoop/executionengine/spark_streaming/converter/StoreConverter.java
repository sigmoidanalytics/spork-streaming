package org.apache.pig.backend.hadoop.executionengine.spark_streaming.converter;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
//import org.apache.hadoop.io.Text;
import org.apache.pig.backend.hadoop.executionengine.spark_streaming.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigOutputFormat;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.spark_streaming.SparkUtil;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.util.ObjectSerializer;

import scala.Function1;
import scala.Tuple2;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;
import twitter4j.Status;

import org.apache.spark.rdd.PairRDDFunctions;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.dstream.DStream;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import com.google.common.collect.Lists;

/**
 * Converter that takes a POStore and stores it's content.
 *
 * @author billg
 */
@SuppressWarnings({ "serial"})
public class StoreConverter implements POConverter<Tuple, Tuple2<Text, Tuple>, POStore> {
	private static final Log LOG = LogFactory.getLog(GlobalRearrangeConverter.class);
	private static final FromTupleFunction FROM_TUPLE_FUNCTION = new FromTupleFunction();

	private static final Function1<Tuple2<Text, Tuple>, Tuple> TO_VALUE_FUNCTION = new ToTupleFunction();

	private PigContext pigContext;

	public StoreConverter(PigContext pigContext) {
		this.pigContext = pigContext;
	}


	@Override
	public JavaDStream<Tuple> convert(List<JavaDStream<Tuple>> predecessors, POStore physicalOperator) throws IOException {

		SparkUtil.assertPredecessorSize(predecessors, physicalOperator, 1);
		JavaDStream<Tuple> rdd = predecessors.get(0);

		final POStore poperator = physicalOperator;

		//JobConf storeJobConf = SparkUtil.newJobConf(pigContext);
		//POStore poStore = configureStorer(storeJobConf, physicalOperator);

		//rdd.print();

		System.out.println("DCount ====>>>" +  rdd.dstream().count());
		testFunction fnc = new testFunction();

		/*		
		DStream<Text> dts = rdd.dstream().map(fnc, SparkUtil.getManifest(Text.class));
		dts.print();
		dts.saveAsTextFiles(poStore.getSFile().getFileName(), "" + System.currentTimeMillis());
		*/

		//rdd.dstream().saveAsObjectFiles(poStore.getSFile().getFileName(), "" + System.currentTimeMillis());


		DStream<Tuple2<Text, Tuple>> dstatuses = rdd.dstream().map(FROM_TUPLE_FUNCTION, SparkUtil.<Text, Tuple>getTuple2Manifest());

		//dstatuses.print();

		dstatuses.foreachRDD(
				new Function<RDD<Tuple2<Text, Tuple>>,BoxedUnit>(){
					@Override
					public BoxedUnit call(RDD<Tuple2<Text, Tuple>> rdd) throws Exception {

						try{

							PairRDDFunctions<Text, Tuple> pairRDDFunctions = new PairRDDFunctions<Text, Tuple>(rdd,
									SparkUtil.getManifest(Text.class), SparkUtil.getManifest(Tuple.class));

							JobConf storeJobConf = SparkUtil.newJobConf(pigContext);
							POStore poStore = configureStorer(storeJobConf, poperator);

							pairRDDFunctions.saveAsNewAPIHadoopFile(poStore.getSFile().getFileName(),Text.class, Tuple.class, PigOutputFormat.class, storeJobConf);


						}catch(Exception e){
							System.out.println("CRASSSSSHHHHHHHHH");
							e.printStackTrace();
						}

						return null;
					}                        
				}
				);

		JavaDStream<Tuple> hdfsTuple = new JavaDStream<Tuple>(dstatuses.map(TO_VALUE_FUNCTION,SparkUtil.getManifest(Tuple.class)),SparkUtil.getManifest(Tuple.class));

		return hdfsTuple;
	}

	private static class testFunction extends Function<Tuple,Text> implements Serializable {
		@Override
		public Text call(Tuple status) throws Exception {

			Text t = new Text(status.toString());
			return t;
		}
	}

	private static POStore configureStorer(JobConf jobConf,
			PhysicalOperator physicalOperator) throws IOException {
		ArrayList<POStore> storeLocations = Lists.newArrayList();
		POStore poStore = (POStore)physicalOperator;
		storeLocations.add(poStore);
		StoreFuncInterface sFunc = poStore.getStoreFunc();
		sFunc.setStoreLocation(poStore.getSFile().getFileName(), new org.apache.hadoop.mapreduce.Job(jobConf));
		poStore.setInputs(null);
		poStore.setParentPlan(null);

		jobConf.set(JobControlCompiler.PIG_MAP_STORES, ObjectSerializer.serialize(Lists.newArrayList()));
		jobConf.set(JobControlCompiler.PIG_REDUCE_STORES, ObjectSerializer.serialize(storeLocations));

		//jobConf.set("org.apache.hadoop.io.Text", ObjectSerializer.serialize(org.apache.hadoop.io.Text.class));

		return poStore;
	}

	private static class ToTupleFunction extends AbstractFunction1<Tuple2<Text, Tuple>,Tuple>
	implements Function1<Tuple2<Text, Tuple>,Tuple>, Serializable {

		@Override
		public Tuple apply(Tuple2<Text, Tuple> v1) {
			return v1._2();
		}
	}

	private static class FromTupleFunction extends Function<Tuple, Tuple2<Text, Tuple>>
	implements Serializable {

		private static Text EMPTY_TEXT = new Text();

		public Tuple2<Text, Tuple> call(Tuple v1) {
			return new Tuple2<Text, Tuple>(EMPTY_TEXT, v1);
		}
	}

}
