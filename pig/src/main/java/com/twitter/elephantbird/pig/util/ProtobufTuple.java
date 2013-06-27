package com.twitter.elephantbird.pig.util;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;
import com.twitter.elephantbird.pig.util.ProtobufToPig;

@SuppressWarnings("serial")
/**
 * This class wraps a protocol buffer message and attempts to delay parsing until individual
 * fields are requested.
 */
public class ProtobufTuple extends AbstractLazyTuple {

  private final Message msg_;
  private final Descriptor descriptor_;
  private final List<FieldDescriptor> fieldDescriptors_;
  private final ProtobufToPig protoConv_;
  private final int protoSize_;

  public ProtobufTuple(Message msg) {
    msg_ = msg;
    descriptor_ = msg.getDescriptorForType();
    fieldDescriptors_ = descriptor_.getFields();
    protoSize_ = fieldDescriptors_.size();
    protoConv_ = new ProtobufToPig();
    initRealTuple(protoSize_);
  }

  protected Object getObjectAt(int idx) {
    FieldDescriptor fieldDescriptor = fieldDescriptors_.get(idx);
    Object fieldValue = msg_.getField(fieldDescriptor);
    return protoConv_.fieldToPig(fieldDescriptor, fieldValue);
  }

  @Override
  public long getMemorySize() {
    // The protobuf estimate is obviously inaccurate.
    return msg_.getSerializedSize() + realTuple.getMemorySize();
  }
}
