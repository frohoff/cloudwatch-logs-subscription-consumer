package com.amazonaws.services.logs.connectors.samples.kinesis.flow;

import java.util.regex.Pattern;

import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.interfaces.IFilter;
import com.amazonaws.services.logs.connectors.samples.kinesis.KinesisPipeline;
import com.amazonaws.services.logs.subscriptions.CloudWatchLogsEvent;

public class FilteredKinesisPipeline extends KinesisPipeline {
	@Override
	public IFilter<CloudWatchLogsEvent> getFilter(KinesisConnectorConfiguration configuration) {
		if (configuration instanceof FilteredKinesisConnectorConfiguration) {
			FilteredKinesisConnectorConfiguration filteredConf = (FilteredKinesisConnectorConfiguration) configuration;
			return new CloudWatchRegexFilter(filteredConf.WHITELISTED_PATTERNS, filteredConf.BLACKLISTED_PATTERNS);
		} else {
			return new CloudWatchRegexFilter(new Pattern[0], new Pattern[0]);
		}
	}
}
