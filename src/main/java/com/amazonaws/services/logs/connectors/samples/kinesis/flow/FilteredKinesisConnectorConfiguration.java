package com.amazonaws.services.logs.connectors.samples.kinesis.flow;

import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;

public class FilteredKinesisConnectorConfiguration extends KinesisConnectorConfiguration {

	public final Pattern[] WHITELISTED_PATTERNS;
	public final Pattern[] BLACKLISTED_PATTERNS;
	public FilteredKinesisConnectorConfiguration(Properties properties, AWSCredentialsProvider credentialsProvider) {
		super(properties, credentialsProvider);
		Set<Entry<Object, Object>> entries = properties.entrySet();
		WHITELISTED_PATTERNS = compilePatterns(entries, "whitelisted.");
		BLACKLISTED_PATTERNS = compilePatterns(entries, "blacklisted.");
	}

	// FIXME: toString() calls are a hack
	private Pattern[] compilePatterns(Set<Entry<Object, Object>> entries, String keyPrefix) {
		List<Pattern> patterns = new LinkedList<Pattern>();
		for (Entry<Object, Object> e : entries) {
			if (e.getKey().toString().startsWith(keyPrefix)) {
				Pattern pattern = Pattern.compile(e.getValue().toString());
				patterns.add(pattern);
			}
		}
		return patterns.toArray(new Pattern[patterns.size()]);
	}

}
