package com.twitter.elephantbird.hive.serde;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.Descriptors.FieldDescriptor.Type;
import com.google.protobuf.Message;

public final class ProtobufStructObjectInspector extends
		SettableStructObjectInspector {

	public static class ProtobufStructField implements StructField {

		private ObjectInspector oi = null;
		private String comment = null;
		private FieldDescriptor fieldDescriptor;

		@SuppressWarnings("unchecked")
		public ProtobufStructField(FieldDescriptor fieldDescriptor) {
			this.fieldDescriptor = fieldDescriptor;
			oi = this.createOIForField();
		}

		@Override
		public String getFieldName() {
			return fieldDescriptor.getName();
		}

		@Override
		public ObjectInspector getFieldObjectInspector() {
			return oi;
		}

		@Override
		public String getFieldComment() {
			return comment;
		}

		public FieldDescriptor getFieldDescriptor() {
			return fieldDescriptor;
		}

		private PrimitiveCategory getPrimitiveCategory(JavaType fieldType) {
			switch (fieldType) {
			case INT:
				return PrimitiveCategory.INT;
			case LONG:
				return PrimitiveCategory.LONG;
			case FLOAT:
				return PrimitiveCategory.FLOAT;
			case DOUBLE:
				return PrimitiveCategory.DOUBLE;
			case BOOLEAN:
				return PrimitiveCategory.BOOLEAN;
			case STRING:
				return PrimitiveCategory.STRING;
			case BYTE_STRING:
				return PrimitiveCategory.BINARY;
			case ENUM:
				return PrimitiveCategory.STRING;
			default:
				return null;
			}
		}

		private ObjectInspector createOIForField() {
			JavaType fieldType = fieldDescriptor.getJavaType();
			PrimitiveCategory category = getPrimitiveCategory(fieldType);
			ObjectInspector elementOI = null;
			if (category != null) {
				elementOI = PrimitiveObjectInspectorFactory
						.getPrimitiveJavaObjectInspector(category);
			} else {
				switch (fieldType) {
				case MESSAGE:
					elementOI = new ProtobufStructObjectInspector(
							fieldDescriptor.getMessageType());
					break;
				default:
					throw new RuntimeException("JavaType " + fieldType
							+ " from protobuf is not supported.");
				}
			}
			if (fieldDescriptor.isRepeated()) {
				return ObjectInspectorFactory
						.getStandardListObjectInspector(elementOI);
			} else {
				return elementOI;
			}
		}
	}

	private Descriptor descriptor;
	private List<StructField> structFields = Lists.newArrayList();

	ProtobufStructObjectInspector(Descriptor descriptor) {
		this.descriptor = descriptor;
		for (FieldDescriptor fd : descriptor.getFields()) {
			structFields.add(new ProtobufStructField(fd));
		}
	}

	@Override
	public Category getCategory() {
		return Category.STRUCT;
	}

	@Override
	public String getTypeName() {
		StringBuilder sb = new StringBuilder("struct<");
		boolean first = true;
		for (StructField structField : getAllStructFieldRefs()) {
			if (first) {
				first = false;
			} else {
				sb.append(",");
			}
			sb.append(structField.getFieldName())
					.append(":")
					.append(structField.getFieldObjectInspector().getTypeName());
		}
		sb.append(">");
		return sb.toString();
	}

	@Override
	public Object create() {
		return descriptor.toProto().toBuilder().build();
	}

	@Override
	public Object setStructFieldData(Object data, StructField field,
			Object fieldValue) {
		return ((Message) data)
				.toBuilder()
				.setField(descriptor.findFieldByName(field.getFieldName()),
						fieldValue).build();
	}

	@Override
	public List<? extends StructField> getAllStructFieldRefs() {
		return structFields;
	}

	@Override
	public Object getStructFieldData(Object data, StructField structField) {
		if (data == null) {
			return null;
		}
		Message m = (Message) data;
		ProtobufStructField psf = (ProtobufStructField) structField;
		FieldDescriptor fieldDescriptor = psf.getFieldDescriptor();
		Object result = m.getField(fieldDescriptor);

		return toJava(fieldDescriptor, result);
	}

	private Object toJava(FieldDescriptor fieldDescriptor, Object result) {

		if (result instanceof Collection) {

			final Collection coll = (Collection) result;
			final List l = new ArrayList();
			final Iterator it = coll.iterator();

			while (it.hasNext())
				l.add(toJava(fieldDescriptor, it.next()));

			return result;
		} else if (result instanceof EnumValueDescriptor) {
			return ((EnumValueDescriptor) result).getName();
		} else if (result instanceof ByteString) {
			return ((ByteString) result).toStringUtf8();
		}else
			return null;
	}

	@Override
	public StructField getStructFieldRef(String fieldName) {
		return new ProtobufStructField(descriptor.findFieldByName(fieldName));
	}

	@Override
	public List<Object> getStructFieldsDataAsList(Object data) {
		if (data == null) {
			return null;
		}
		List<Object> result = Lists.newArrayList();
		Message m = (Message) data;
		for (FieldDescriptor fd : descriptor.getFields()) {
			result.add(m.getField(fd));
		}
		return result;
	}
}
