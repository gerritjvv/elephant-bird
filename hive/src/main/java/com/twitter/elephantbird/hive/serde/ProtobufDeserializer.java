package com.twitter.elephantbird.hive.serde;

import java.util.Properties;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import com.twitter.elephantbird.mapreduce.io.ProtobufConverter;
import com.twitter.elephantbird.util.Protobufs;

/**
 * A deserializer for protobufs. Expects protbuf serialized bytes as
 * BytesWritables from input format. <p>
 *
 * Usage: <pre>
 *  create table users
 *    row format serde "com.twitter.elephantbird.hive.serde.ProtobufDeserializer"
 *    with serdeproperties (
 *        "serialization.class"="com.example.proto.gen.Storage$User")
 *    stored as
 *    inputformat "com.twitter.elephantbird.mapred.input.DeprecatedRawMultiInputFormat"
  *  ;
 * </pre>
 */
public class ProtobufDeserializer implements Deserializer {

  private ProtobufConverter<? extends Message> protobufConverter = null;
  private ObjectInspector objectInspector;
  
  
  @Override
  public void initialize(Configuration job, Properties tbl) throws SerDeException {
    try {
      String protoClassName = tbl
          .getProperty(org.apache.hadoop.hive.serde.Constants.SERIALIZATION_CLASS);

      Class<? extends Message> protobufClass = job.getClassByName(protoClassName)
          .asSubclass(Message.class);
      protobufConverter = ProtobufConverter.newInstance(protobufClass);

      Descriptor descriptor = Protobufs.getMessageDescriptor(protobufClass);
      objectInspector = new ProtobufStructObjectInspector(descriptor);
    } catch (Exception e) {
      throw new SerDeException(e);
    }
  }
  
  @Override
  public Object deserialize(Writable blob) throws SerDeException {
	Text t = (Text)blob;
	
	byte[] data = Base64.decodeBase64(t.getBytes());
			
    return protobufConverter.fromBytes(data, 0, data.length);
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return objectInspector;
  }

  @Override
  public SerDeStats getSerDeStats() {
    // no support for statistics
    return null;
  }
}
