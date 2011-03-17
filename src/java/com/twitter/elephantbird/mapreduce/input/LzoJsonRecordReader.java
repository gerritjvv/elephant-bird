package com.twitter.elephantbird.mapreduce.input;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads line from an lzo compressed text file, and decodes each line into a json map.
 * Skips lines that are invalid json.
 *
 * WARNING: Does not handle multi-line json input well, if at all.
 * TODO: Fix that, and keep Hadoop counters for invalid vs. valid lines.
 */
public class LzoJsonRecordReader extends LzoRecordReader<LongWritable, MapWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(LzoJsonRecordReader.class);

  private LineReader in_;

  private final LongWritable key_ = new LongWritable();
  private final Text currentLine_ = new Text();
  private final MapWritable value_ = new MapWritable();
  private final JSONParser jsonParser_ = new JSONParser();

  @Override
  public synchronized void close() throws IOException {
    if (in_ != null) {
      in_.close();
    }
  }

  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException {
    return key_;
  }

  @Override
  public MapWritable getCurrentValue() throws IOException, InterruptedException {
    return value_;
  }

  @Override
  protected void createInputReader(InputStream input, Configuration conf) throws IOException {
    in_ = new LineReader(input, conf);
  }

  @Override
  protected void skipToNextSyncPoint(boolean atFirstRecord) throws IOException {
    if (!atFirstRecord) {
      in_.readLine(new Text());
    }
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    // Since the lzop codec reads everything in lzo blocks, we can't stop if pos == end.
    // Instead we wait for the next block to be read in, when pos will be > end.
    value_.clear();

    while (pos_ <= end_) {
      key_.set(pos_);

      int newSize = in_.readLine(currentLine_);
      if (newSize == 0) {
        return false;
      }

      pos_ = getLzoFilePos();

      if (!decodeLineToJson()) {
        continue;
      }

      return true;
    }

    return false;
  }

  private MapWritable walkJson(JSONObject jsonObj) {
    MapWritable v = new MapWritable();

    try {
      for (Object key: jsonObj.keySet()) {
        Text mapKey = new Text(key.toString());
        Object value = jsonObj.get(key);
        if (value instanceof String) {
          Text mapValue = new Text(value.toString());
          v.put(mapKey, mapValue);
        } else if (value instanceof Number) {
          LongWritable mapValue = new LongWritable(((Number) value).longValue());
          v.put(mapKey, mapValue);
        } else if (value instanceof Map) {
          MapWritable mapValue = walkJson((JSONObject) value);
          v.put(mapKey, mapValue);
        } // else if (value instanceof List) {
          // TupleWritable mapValue = new TupleWritable((Writable[]) ((List) value).toArray());
          //v.put(mapKey, mapValue);
        // }
      }
    } catch (NumberFormatException e) {
      return null;
    };

    return v;
  }

  protected boolean decodeLineToJson() {
    try {
      JSONObject jsonObj = (JSONObject)jsonParser_.parse(currentLine_.toString());
      MapWritable jsonMap = walkJson(jsonObj);
      value_.putAll(jsonMap);
      return true;
    } catch (ParseException e) {
      LOG.warn("Could not json-decode string: " + currentLine_, e);
      return false;
    } catch (NumberFormatException e) {
      LOG.warn("Could not parse field into number: " + currentLine_, e);
      return false;
    }
  }
}

