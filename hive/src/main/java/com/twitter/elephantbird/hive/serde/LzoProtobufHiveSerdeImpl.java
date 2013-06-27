package com.twitter.elephantbird.hive.serde;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Writable;

public class LzoProtobufHiveSerdeImpl extends LzoProtobufHiveSerde {

	ProtobufDeserializer des = new ProtobufDeserializer();
	
	@Override
	public void initialize(Configuration conf, Properties props)
			throws SerDeException {
		
		String clsName = props.getProperty("serialization.class");

		if (clsName == null)
			throw new RuntimeException(
					"The serialization.class property must be defined");

		// protobuffs.getMessageBuilder
		des.initialize(conf, props);
		
	}

	@Override
	public ObjectInspector getObjectInspector() throws SerDeException {
		return des.getObjectInspector();
	}

	@Override
	public Object deserialize(Writable w) throws SerDeException {
		return des.deserialize(w);
	}

}
