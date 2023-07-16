/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.jnj.adf.springxd.export.parquet.json;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.jnj.adf.springxd.export.parquet.reader.bean.SimpleRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.codehaus.jackson.map.ObjectMapper;

import com.jnj.adf.client.api.JsonObject;

public abstract class JsonRecordFormatter<T> {
	private static final int SINGLE_VALUE = 0;

	@SuppressWarnings("unused")
	private static final ObjectMapper mapper = new ObjectMapper();
	
	@SuppressWarnings("unused")
	private static final Log logger = LogFactory.getLog(JsonRecordFormatter.class);

	public static class JsonPrimitiveWriter extends JsonRecordFormatter<Object> {

		public JsonPrimitiveWriter(Type primitiveType) {
			super(primitiveType);
		}

		@Override
		protected Object formatResults(List<Object> listOfValues) {
			if (super.typeInfo.getRepetition() == Type.Repetition.REPEATED) {
				return listOfValues;
			} else {
				return listOfValues.get(SINGLE_VALUE);
			}
		}
	}

	public static class JsonGroupFormatter extends JsonRecordFormatter<SimpleRecord> {
		@SuppressWarnings("rawtypes")
		private final Map<String, JsonRecordFormatter> formatters;

		public JsonGroupFormatter(GroupType schema) {
			super(schema);

			this.formatters = buildWriters(schema);
		}

		@SuppressWarnings("rawtypes")
		private Map<String, JsonRecordFormatter> buildWriters(GroupType groupSchema) {
			Map<String, JsonRecordFormatter> writers = new LinkedHashMap<>();
			for (Type type : groupSchema.getFields()) {
				if (type.isPrimitive()) {
					writers.put(type.getName(), new JsonPrimitiveWriter(type));
				} else {
					writers.put(type.getName(), new JsonGroupFormatter((GroupType) type));
				}
			}

			return writers;
		}

		private Map<String, Object> add(SimpleRecord record) {
			return formatEntries(collateEntries(record));
		}

		private Map<String, List<Object>> collateEntries(SimpleRecord record) {
			Map<String, List<Object>> collatedEntries = new LinkedHashMap<>();
			for (SimpleRecord.NameValue value : record.getValues()) {
				if (collatedEntries.containsKey(value.getName())) {
					collatedEntries.get(value.getName()).add(value.getValue());
				} else {
					List<Object> newResultListForKey = new ArrayList<>();
					newResultListForKey.add(value.getValue());
					collatedEntries.put(value.getName(), newResultListForKey);
				}
			}

			return collatedEntries;
		}

		@SuppressWarnings({ "unchecked", "rawtypes" })
		private Map<String, Object> formatEntries(Map<String, List<Object>> entries) {
			Map<String, Object> results = new LinkedHashMap<>();
			for (Map.Entry<String, List<Object>> entry : entries.entrySet()) {
				JsonRecordFormatter formatter = formatters.get(entry.getKey());
				results.put(entry.getKey(), formatter.formatResults(entry.getValue()));
			}

			return results;
		}

		@Override
		protected Object formatResults(List<SimpleRecord> values) {
			if (super.typeInfo.getRepetition() == Type.Repetition.REPEATED) {
				List<Object> results = new ArrayList<>();
				for (SimpleRecord object : values) {
					results.add(add(object));
				}

				return results;
			} else {
				return add(values.get(SINGLE_VALUE));
			}
		}

		public String formatRecord(SimpleRecord value) throws IOException {
			ObjectMapper mapper = new ObjectMapper();
			return mapper.writeValueAsString(add(value));
		}

		public JsonObject makeRecord(SimpleRecord entry) throws IOException {
			List<SimpleRecord.NameValue> values = entry.getValues();
			JsonObject json = JsonObject.create();
			for (SimpleRecord.NameValue val : values) {
				String value = null;
				Object param = val.getValue();
				if (param instanceof byte[]) {
					value =  new String((byte[]) param,"UTF-8");
				} else if (param instanceof Integer) {
					value = ((Integer) param).toString();
				} else if (param instanceof String) {
					value = (String) param;
				} else if (param instanceof Double) {
					value = ((Double) param).toString();
				} else if (param instanceof Float) {
					value = ((Float) param).toString();
				} else if (param instanceof Long) {
					value = ((Long) param).toString();
				} else if (param instanceof Boolean) {
					value = ((Boolean) param).toString();
				} else if (param instanceof Date) {
					value = ((Date) param).toString();
				}
				json.append(val.getName(), value);
			}
			return json;
		}
	}

	protected final Type typeInfo;

	protected JsonRecordFormatter(Type type) {
		this.typeInfo = type;
	}

	protected abstract Object formatResults(List<T> values);

	public static JsonGroupFormatter fromSchema(MessageType messageType) {
		return new JsonGroupFormatter(messageType);
	}
}
