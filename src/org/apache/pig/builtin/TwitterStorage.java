package org.apache.pig.builtin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.pig.Expression;
import org.apache.pig.FileInputLoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.LoadPushDown;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.StoreMetadata;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import twitter4j.Status;

public class TwitterStorage extends FileInputLoadFunc implements StoreFuncInterface,
LoadPushDown, LoadMetadata, StoreMetadata{
	

	public TwitterStorage(){
		
		System.out.println("======TwitterStorage======");
		
	}

	@Override
	public void storeStatistics(ResourceStatistics stats, String location,
			Job job) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void storeSchema(ResourceSchema schema, String location, Job job)
			throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public ResourceSchema getSchema(String location, Job job)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResourceStatistics getStatistics(String location, Job job)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String[] getPartitionKeys(String location, Job job)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setPartitionFilter(Expression partitionFilter)
			throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public List<OperatorSet> getFeatures() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RequiredFieldResponse pushProjection(
			RequiredFieldList requiredFieldList) throws FrontendException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String relToAbsPathForStoreLocation(String location, Path curDir)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public OutputFormat getOutputFormat() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setStoreLocation(String location, Job job) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void checkSchema(ResourceSchema s) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void prepareToWrite(RecordWriter writer) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void putNext(Tuple t) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setStoreFuncUDFContextSignature(String signature) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void cleanupOnFailure(String location, Job job) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void cleanupOnSuccess(String location, Job job) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setLocation(String location, Job job) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public InputFormat getInputFormat() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void prepareToRead(RecordReader reader, PigSplit split)
			throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Tuple getNext() throws IOException {
		// TODO Auto-generated method stub
		
		System.out.println("======TwitterStorage=====");
		
		ArrayList<String> al = new ArrayList<String>();
		al.add("##########Lulz");
		TupleFactory mTupleFactory = TupleFactory.getInstance();
		Tuple t =  mTupleFactory.newTupleNoCopy(al);
   		    		
		
		return t;
	
	}

}
