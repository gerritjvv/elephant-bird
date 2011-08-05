package com.twitter.elephantbird.pig.proto;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.google.common.collect.Sets;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;
import com.twitter.elephantbird.pig.util.ProtobufToPig;

/**
 * This class wraps a protocol buffer message and attempts to delay parsing
 * until individual fields are requested.
 */
public class ProtobufTuple implements Tuple {

	private static final long serialVersionUID = 8468589454361280269L;
	private Tuple realTuple;

	private static final ProtobufToPig protoToPig = new ProtobufToPig();
	
	Message msg;
	int[] requiredColumns;
	
	public ProtobufTuple(){

		realTuple = TupleFactory.getInstance().newTuple();
		
	}
	
	public ProtobufTuple(Message msg, int[] requiredColumns) throws IOException {
		if(msg == null){
			this.msg = null;
			realTuple = null;
			
		}else{
			try{
		this.msg = msg;
		this.requiredColumns = requiredColumns;
		
		Descriptor descriptor = msg.getDescriptorForType();
		List<FieldDescriptor> fieldDescriptors = descriptor.getFields();
		
		//the tuple length depends if we have a required columns list (i.e. column pruning is used)
		//or none.
		

		
		
		if(requiredColumns == null || requiredColumns.length <= 0){
			int len = fieldDescriptors.size();
			realTuple = TupleFactory.getInstance().newTuple(len);
			
			for(int i = 0; i < len; i++){
			         copyMessageValueToTuple(i, i, fieldDescriptors, msg, realTuple);
			}
			
		}else{
			int len = requiredColumns.length;
			realTuple = TupleFactory.getInstance().newTuple(len);
			
			for(int i = 0; i < len; i++){
		         copyMessageValueToTuple(requiredColumns[i], i, fieldDescriptors, msg, realTuple);
			}
		
			
		}
			}catch(NullPointerException nullPionter){
				System.out.println(nullPionter);
			}
		}
		
	}

	private static final void copyMessageValueToTuple(int index, int tupleIndex, List<FieldDescriptor> fieldDescriptors, Message msg,
			Tuple tuple) throws IOException {
		//get message
		FieldDescriptor fieldDescriptor = fieldDescriptors.get(index);
		
		Object fieldValue = msg.getField(fieldDescriptor);
		if (fieldDescriptor.getType() == FieldDescriptor.Type.MESSAGE) {
			tuple.set(tupleIndex,
					protoToPig.messageToTuple(fieldDescriptor, fieldValue));
		} else {
			tuple.set(tupleIndex, protoToPig.singleFieldToTuple(
					fieldDescriptor, fieldValue));
		}
	}

	public ProtobufTuple(Message msg) throws IOException {
		this(msg, null);
	}

	@Override
	public void append(Object obj) {
		if(msg == null) return;
		realTuple.append(obj);
	}

	
	@Override
	public List<Object> getAll() {
		try{
		if(msg == null) return null;
		return realTuple.getAll();
		}catch(NullPointerException e){
			return new ArrayList<Object>();
		}
	}

	@Override
	public long getMemorySize() {
		if(msg == null) return 0L;
		// The protobuf estimate is obviously inaccurate.
		return msg.getSerializedSize() + realTuple.getMemorySize();
	}

	@Override
	public byte getType(int idx) throws ExecException {
		if(msg == null) return (byte)0;
		
		return realTuple.getType(idx);
	}

	@Override
	public boolean isNull() {
		if(msg == null) return true;
		return realTuple.isNull();
	}

	@Override
	public boolean isNull(int idx) throws ExecException {
		if(msg == null) return true;
		return realTuple.isNull(idx);
	}

	@Override
	public void reference(Tuple arg) {
		if(msg == null) return;
		realTuple.reference(arg);
		// Ignore the Message from now on.
	}

	@Override
	public void set(int idx, Object val) throws ExecException {
		if(msg == null) return;
		realTuple.set(idx, val);
	}

	@Override
	public void setNull(boolean isNull) {
		if(msg == null) return;
		realTuple.setNull(isNull);
	}

	@Override
	public int size() {
		if(msg == null) return 0;
		return realTuple.size();
	}

	@Override
	public String toDelimitedString(String delim) throws ExecException {
		if(msg == null) return null;
		return realTuple.toDelimitedString(delim);
	}

	@Override
	public void readFields(DataInput inp) throws IOException {
		Builder builder = msg.newBuilderForType();
		try {
			builder.mergeDelimitedFrom((DataInputStream) inp);
		} catch (ClassCastException e) {
			throw new IOException(
					"Provided DataInput not instance of DataInputStream.", e);
		}
		Message msg = builder.build();
		realTuple.reference(new ProtobufTuple(msg, requiredColumns));
	}

	@Override
	public void write(DataOutput out) throws IOException {
		if(msg == null) return;
		realTuple.write(out);
	}

	@SuppressWarnings("unchecked")
	@Override
	public int compareTo(Object obj) {
		if(msg == null) return -1;
		return realTuple.compareTo(obj);
	}

	
	@Override
	public Object get(int index) throws ExecException {
		if(msg == null) return null;
		return realTuple.get(index);
	}
}
