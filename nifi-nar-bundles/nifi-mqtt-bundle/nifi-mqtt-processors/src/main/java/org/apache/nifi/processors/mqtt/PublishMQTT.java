/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.processors.mqtt;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.mqtt.common.AbstractMQTTProcessor;
import org.apache.nifi.processors.mqtt.common.MqttCallback;
import org.apache.nifi.processors.mqtt.common.ReceivedMqttMessage;
import org.apache.nifi.processors.mqtt.common.StandardMqttMessage;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.StopWatch;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Optional.ofNullable;

@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"publish", "MQTT", "IOT"})
@CapabilityDescription("Publishes a message to an MQTT topic")
@SeeAlso({ConsumeMQTT.class})
@SystemResourceConsideration(resource = SystemResource.MEMORY)
public class PublishMQTT extends AbstractMQTTProcessor implements MqttCallback {

    public static final PropertyDescriptor PROP_TOPIC = new PropertyDescriptor.Builder()
            .name("Topic")
            .description("The topic to publish the message to.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_QOS = new PropertyDescriptor.Builder()
            .name("Quality of Service(QoS)")
            .displayName("Quality of Service (QoS)")
            .description("The Quality of Service (QoS) to send the message with. Accepts three values '0', '1' and '2'; '0' for 'at most once', '1' for 'at least once', '2' for 'exactly once'. " +
                    "Expression language is allowed in order to support publishing messages with different QoS but the end value of the property must be either '0', '1' or '2'. ")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(QOS_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_RETAIN = new PropertyDescriptor.Builder()
            .name("Retain Message")
            .description("Whether or not the retain flag should be set on the MQTT message.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(RETAIN_VALIDATOR)
            .build();

    public static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(BASE_RECORD_READER)
            .description("The Record Reader to use for parsing the incoming FlowFile into Records.")
            .build();

    public static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(BASE_RECORD_WRITER)
            .description("The Record Writer to use for serializing Records before publishing them as an MQTT Message.")
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that are sent successfully to the destination are transferred to this relationship.")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that failed to send to the destination are transferred to this relationship.")
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            PROP_BROKER_URI,
            PROP_MQTT_VERSION,
            PROP_USERNAME,
            PROP_PASSWORD,
            PROP_SSL_CONTEXT_SERVICE,
            PROP_CLEAN_SESSION,
            PROP_SESSION_EXPIRY_INTERVAL,
            PROP_CLIENTID,
            PROP_TOPIC,
            PROP_RETAIN,
            PROP_QOS,
            RECORD_READER,
            RECORD_WRITER,
            PROP_CONN_TIMEOUT,
            PROP_KEEP_ALIVE_INTERVAL,
            PROP_LAST_WILL_MESSAGE,
            PROP_LAST_WILL_TOPIC,
            PROP_LAST_WILL_RETAIN,
            PROP_LAST_WILL_QOS
    ));

    private static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            REL_SUCCESS,
            REL_FAILURE
    )));

    static final String PROVENANCE_EVENT_DETAILS_ON_RECORDSET_FAILURE = "Publish failed after %d successfully published records.";
    static final String PROVENANCE_EVENT_DETAILS_ON_RECORDSET_RECOVER = "Successfully finished publishing previously failed records. Total record count: %d";
    static final String PROVENANCE_EVENT_DETAILS_ON_RECORDSET_SUCCESS = "Successfully published all records. Total record count: %d";

    static final String ATTR_PUBLISH_FAILED_INDEX_SUFFIX = ".mqtt.publish.failed.index";
    private String publishFailedIndexAttributeName;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        logger = getLogger();
        publishFailedIndexAttributeName = getIdentifier() + ATTR_PUBLISH_FAILED_INDEX_SUFFIX;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        super.onScheduled(context);
    }

    @OnStopped
    public void onStopped(final ProcessContext context) {
        synchronized (this) {
            stopClient();
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile flowfile = session.get();
        if (flowfile == null) {
            return;
        }

        if (!isConnected()) {
            synchronized (this) {
                if (!isConnected()) {
                    initializeClient(context);
                }
            }
        }

        // get the MQTT topic
        final String topic = context.getProperty(PROP_TOPIC).evaluateAttributeExpressions(flowfile).getValue();

        if (topic == null || topic.isEmpty()) {
            logger.warn("Evaluation of the topic property returned null or evaluated to be empty, routing to failure");
            session.transfer(flowfile, REL_FAILURE);
            return;
        }

        if (context.getProperty(RECORD_READER).isSet()) {
            processRecordSet(context, session, flowfile, topic);
        } else {
            processStandardFlowFile(context, session, flowfile, topic);
        }
    }

    private void processRecordSet(ProcessContext context, ProcessSession session, final FlowFile flowfile, String topic) {
        final StopWatch stopWatch = new StopWatch(true);
        final AtomicInteger processedRecords = new AtomicInteger();

        try {
            final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
            final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

            final Long previousProcessFailedAt = ofNullable(flowfile.getAttribute(publishFailedIndexAttributeName)).map(Long::valueOf).orElse(null);

            session.read(flowfile, in -> {
                try (final RecordReader reader = readerFactory.createRecordReader(flowfile, in, logger)) {
                    final RecordSet recordSet = reader.createRecordSet();

                    final RecordSchema schema = writerFactory.getSchema(flowfile.getAttributes(), recordSet.getSchema());

                    final ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);

                    Record record;
                    while ((record = recordSet.next()) != null) {
                        if (previousProcessFailedAt != null && processedRecords.get() < previousProcessFailedAt) {
                            processedRecords.getAndIncrement();
                            continue;
                        }

                        baos.reset();

                        try (final RecordSetWriter writer = writerFactory.createWriter(logger, schema, baos, flowfile)) {
                            writer.write(record);
                            writer.flush();
                        }

                        final byte[] messageContent = baos.toByteArray();

                        publishMessage(context, flowfile, topic, messageContent);
                        processedRecords.getAndIncrement();
                    }
                } catch (SchemaNotFoundException | MalformedRecordException e) {
                    throw new ProcessException("An error happened during creating components for serialization.", e);
                }
            });

            FlowFile successFlowFile = flowfile;

            String provenanceEventDetails;
            if (previousProcessFailedAt != null) {
                successFlowFile = session.removeAttribute(flowfile, publishFailedIndexAttributeName);
                provenanceEventDetails = String.format(PROVENANCE_EVENT_DETAILS_ON_RECORDSET_RECOVER, processedRecords.get());
            } else {
                provenanceEventDetails = String.format(PROVENANCE_EVENT_DETAILS_ON_RECORDSET_SUCCESS, processedRecords.get());
            }

            session.getProvenanceReporter().send(flowfile, brokerUris.getCurrentElement().toString(), provenanceEventDetails, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            session.transfer(successFlowFile, REL_SUCCESS);
        } catch (Exception e) {
            logger.error("An error happened during publishing records. Routing to failure.", e);

            FlowFile failedFlowFile = session.putAttribute(flowfile, publishFailedIndexAttributeName, String.valueOf(processedRecords.get()));

            if (processedRecords.get() > 0) {
                session.getProvenanceReporter().send(
                        failedFlowFile,
                        brokerUris.getCurrentElement().toString(),
                        String.format(PROVENANCE_EVENT_DETAILS_ON_RECORDSET_FAILURE, processedRecords.get()),
                        stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            }

            session.transfer(failedFlowFile, REL_FAILURE);
        }
    }

    private void processStandardFlowFile(ProcessContext context, ProcessSession session, FlowFile flowfile, String topic) {
        try {
            final byte[] messageContent = new byte[(int) flowfile.getSize()];
            session.read(flowfile, in -> StreamUtils.fillBuffer(in, messageContent, true));

            final StopWatch stopWatch = new StopWatch(true);
            publishMessage(context, flowfile, topic, messageContent);
            session.getProvenanceReporter().send(flowfile, brokerUris.getCurrentElement().toString(), stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            session.transfer(flowfile, REL_SUCCESS);
        } catch (Exception e) {
            logger.error("An error happened during publishing a message. Routing to failure.", e);
            session.transfer(flowfile, REL_FAILURE);
        }
    }

    private void publishMessage(ProcessContext context, FlowFile flowfile, String topic, byte[] messageContent) {
        int qos = context.getProperty(PROP_QOS).evaluateAttributeExpressions(flowfile).asInteger();
        boolean retained = context.getProperty(PROP_RETAIN).evaluateAttributeExpressions(flowfile).asBoolean();
        final StandardMqttMessage mqttMessage = new StandardMqttMessage(messageContent, qos, retained);

        mqttClient.publish(topic, mqttMessage);
    }

    private void initializeClient(ProcessContext context) {
        // NOTE: This method is called when isConnected returns false which can happen when the client is null, or when it is
        // non-null but not connected, so we need to handle each case and only create a new client when it is null
        try {
            mqttClient = createMqttClient();
            mqttClient.setCallback(this);
            mqttClient.connect();
        } catch (Exception e) {
            logger.error("Connection to {} lost (or was never connected) and connection failed. Yielding processor", brokerUris.getCurrentElement().toString(), e);
            context.yield();
        }
    }

    @Override
    public void connectionLost(Throwable cause) {
        logger.error("Connection to {} lost", brokerUris.getCurrentElement().toString(), cause);
    }

    @Override
    public void messageArrived(ReceivedMqttMessage message) {
        // Unlikely situation. Api uses the same callback for publisher and consumer as well.
        // That's why we have this log message here to indicate something really messy thing happened.
        logger.error("Message arrived to a PublishMQTT processor { topic:'" + message.getTopic() + "; payload:" + Arrays.toString(message.getPayload()) + "}");
    }

    @Override
    public void deliveryComplete(String token) {
        // Client.publish waits for message to be delivered so this token will always have a null message and is useless in this application.
        logger.trace("Received 'delivery complete' message from broker. Token: [{}]", token);
    }

}
