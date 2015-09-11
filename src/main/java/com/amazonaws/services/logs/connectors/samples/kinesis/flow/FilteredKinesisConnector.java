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
package com.amazonaws.services.logs.connectors.samples.kinesis.flow;

import java.util.Properties;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorRecordProcessorFactory;
import com.amazonaws.services.kinesis.connectors.kinesis.KinesisEmitter.Record;
import com.amazonaws.services.logs.connectors.samples.AbstractConnectorExecutor;
import com.amazonaws.services.logs.subscriptions.CloudWatchLogsEvent;

public class FilteredKinesisConnector extends AbstractConnectorExecutor<Record> {

    private static String CONFIG_FILE = FilteredKinesisConnector.class.getSimpleName() + ".properties";

    public FilteredKinesisConnector(String configFile) {
        super(configFile);
    }

    @Override
    public KinesisConnectorRecordProcessorFactory<CloudWatchLogsEvent, Record> getKinesisConnectorRecordProcessorFactory() {
        return new KinesisConnectorRecordProcessorFactory<CloudWatchLogsEvent, Record>(new FilteredKinesisPipeline(), getConfig());
    }

    public static void main(String[] args) {
        FilteredKinesisConnector executor = new FilteredKinesisConnector(CONFIG_FILE);
        executor.run();
    }

    @Override
    protected KinesisConnectorConfiguration createConfig(Properties properties) {
    	return new FilteredKinesisConnectorConfiguration(properties, new DefaultAWSCredentialsProviderChain());
    }
}
