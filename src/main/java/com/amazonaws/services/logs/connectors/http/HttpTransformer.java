package com.amazonaws.services.logs.connectors.http;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.apache.commons.lang.NotImplementedException;

import com.amazonaws.services.kinesis.connectors.interfaces.IBatchingCollectionTransformer;
import com.amazonaws.services.logs.subscriptions.CloudWatchLogsEvent;
import com.amazonaws.services.logs.subscriptions.CloudWatchLogsSubscriptionTransformer;

public class HttpTransformer extends CloudWatchLogsSubscriptionTransformer<String> implements IBatchingCollectionTransformer<CloudWatchLogsEvent, String> {

	@Override
	public String fromClass(CloudWatchLogsEvent record) throws IOException {
		throw new NotImplementedException("use fromClasses() method");
	}

	@Override
	public Collection<String> fromClasses(Collection<CloudWatchLogsEvent> records) throws IOException {
		String jsonString = createJsonString(records);
		if (jsonString != null) {
			return Arrays.asList(jsonString);
		} else {
			return new ArrayList<String>();
		}
	}

}
