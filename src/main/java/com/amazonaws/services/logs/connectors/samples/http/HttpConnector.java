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
package com.amazonaws.services.logs.connectors.samples.http;

import com.amazonaws.services.kinesis.connectors.KinesisConnectorRecordProcessorFactory;
import com.amazonaws.services.logs.connectors.samples.AbstractConnectorExecutor;
import com.amazonaws.services.logs.subscriptions.CloudWatchLogsEvent;

public class HttpConnector extends AbstractConnectorExecutor<String> {

    private static String CONFIG_FILE = HttpConnector.class.getSimpleName() + ".properties";

    public HttpConnector(String configFile) {
        super(configFile);
    }

    @Override
    public KinesisConnectorRecordProcessorFactory<CloudWatchLogsEvent, String> getKinesisConnectorRecordProcessorFactory() {
        return new KinesisConnectorRecordProcessorFactory<CloudWatchLogsEvent, String>(
                new HttpPipeline(), getConfig());
    }

    public static void main(String[] args) {
        HttpConnector executor = new HttpConnector(CONFIG_FILE);
        executor.run();
    }
}
