package com.amazonaws.services.logs.connectors.samples.kinesis.flow;

import java.util.regex.Pattern;

import com.amazonaws.services.kinesis.connectors.interfaces.IFilter;
import com.amazonaws.services.logs.subscriptions.CloudWatchLogsEvent;

public class CloudWatchRegexFilter implements IFilter<CloudWatchLogsEvent> {

	private final Pattern[] whitelisted;
	private final Pattern[] blacklisted;

	public CloudWatchRegexFilter(Pattern[] whitelisted, Pattern[] blacklisted) {
		this.whitelisted = whitelisted;
		this.blacklisted = blacklisted;
	}

	@Override
	public boolean keepRecord(CloudWatchLogsEvent record) {
		String message = record.getMessage();
		for (Pattern bl : blacklisted) {
			if (bl.matcher(message).matches()) {
				return false;
			}
		}
		for (Pattern wl: whitelisted) {
			if (wl.matcher(message).matches()) {
				return true;
			}
		}
		return whitelisted.length == 0; // return true if no whitelist
	}
}
