/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.logs.connectors.kinesis;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.connectors.interfaces.IBatchingCollectionTransformer;
import com.amazonaws.services.kinesis.connectors.kinesis.KinesisEmitter;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.logs.subscriptions.CloudWatchLogsEvent;
import com.amazonaws.services.logs.subscriptions.CloudWatchLogsSubscriptionTransformer;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Transforms CloudWatchLogsEvent objects to compressed JSON.
 */
public class KinesisTransformer extends CloudWatchLogsSubscriptionTransformer<Record> implements IBatchingCollectionTransformer<CloudWatchLogsEvent, Record> {

    private static final Log LOG = LogFactory.getLog(KinesisTransformer.class);

    private static final ObjectMapper JSON_OBJECT_MAPPER = new ObjectMapper();

	private static final int MAX_KINESIS_RECORD_SIZE = 1024 * 1024; // 1 MB

	@Override
	public Record fromClass(CloudWatchLogsEvent record) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Collection<Record> fromClasses(Collection<CloudWatchLogsEvent> events) throws IOException {
		LinkedList<Record> records = new LinkedList<Record>();

		LinkedList<CloudWatchLogsEvent> buffer = new LinkedList<CloudWatchLogsEvent>();
		String key = null;
		Iterator<CloudWatchLogsEvent> iter = events.iterator();
		while (iter.hasNext()) {
			CloudWatchLogsEvent event = iter.next();
			String nextKey = getPartitionKey(event);
			if (key != null && ! key.equals(nextKey)) {
				records.addAll(createRecords(buffer));
				buffer.clear();
			}
			buffer.add(event);
		}
		if (buffer.size() > 0) records.addAll(createRecords(buffer));
		return records;
	}

	// recursively split records until within max size
	protected Collection<Record> createRecords(Collection<CloudWatchLogsEvent> events) throws IOException {
		Record one = createRecord(events);
		if (one != null) { // can be serialized properly
			if (one.getData().capacity() > MAX_KINESIS_RECORD_SIZE) {
				if (events.size() == 1) {
					LOG.error("event too large for kinesis record");
					// return empty
				} else {
					// best guess split and retry
					int bytesPerEvent = one.getData().capacity() / events.size();
					// ensure split at least in half
					int maxEventsPerPartition = Math.min(MAX_KINESIS_RECORD_SIZE / bytesPerEvent, events.size() / 2);
					Collection<Collection<CloudWatchLogsEvent>> partitions =
							partition(new ArrayList<CloudWatchLogsEvent>(events), maxEventsPerPartition);
					LinkedList<Record> records = new LinkedList<Record>();
					for (Collection<CloudWatchLogsEvent> partition : partitions) {
						records.addAll(createRecords(partition));
					}
					return records;
				}
			} else {
				return Collections.singleton(one);
			}
		}
		return Collections.emptyList();
	}

	private static <E> Collection<Collection<E>> partition(Collection<E> coll, int maxPerPartition) {
		List<Collection<E>> partitions = new LinkedList<Collection<E>>();
		Iterator<E> iter = coll.iterator();
		while (iter.hasNext()) {
			List<E> partition = new LinkedList<E>();;
			for (int j = 0; iter.hasNext() && j < maxPerPartition; j++) {
				partition.add(iter.next());
			}
			partitions.add(partition);
		}
		return partitions;
	}

	protected Record createRecord(Collection<CloudWatchLogsEvent> events) throws IOException {
		if (events.size() == 0) throw new IllegalArgumentException("events can't be empty");

		ObjectNode root = createJson(events);

		CloudWatchLogsEvent first = events.iterator().next();

		String key = getPartitionKey(first);

		String jsonString = createJsonString(events);

		if (jsonString != null) {
			byte[] compressed = compress(jsonString.getBytes());
			return new Record().withData(ByteBuffer.wrap(compressed)).withPartitionKey(key);
		} else {
			return null;
		}
	}

    protected String getPartitionKey(CloudWatchLogsEvent event) {
    	return event.getOwner() + "-" + event.getLogGroup() + "-" + event.getLogStream();
    }

	private static byte[] compress(byte[] data) throws IOException {
        byte[] buffer = new byte[1024];

        try (ByteArrayInputStream in = new ByteArrayInputStream(data)) {
        	try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                try (GZIPOutputStream gzos = new GZIPOutputStream(out)) {
                    int len;
                    while ((len = in.read(buffer)) > 0) {
                    	gzos.write(buffer, 0, len);
                    }
                    gzos.flush();
                }
                return out.toByteArray();
        	}
        }
    }
}
