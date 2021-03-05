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
package com.fiege.saphir.processors.adx;

import com.fiege.saphir.adx.MyService;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestionMapping;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.result.IngestionStatus;
import com.microsoft.azure.kusto.ingest.result.OperationStatus;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;
import com.microsoft.azure.storage.StorageException;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.microsoft.azure.kusto.ingest.IngestionProperties.IngestionReportMethod.QueueAndTable;

@Tags({"example"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class MyProcessor extends AbstractProcessor {

    public static final PropertyDescriptor MY_PROPERTY = new PropertyDescriptor
            .Builder().name("MY_PROPERTY")
            .displayName("My property")
            .description("Example Property")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DB_NAME = new PropertyDescriptor
            .Builder().name("DB_NAME")
            .displayName("Database name")
            .description("The name of the database to store the data in.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor
            .Builder().name("TABLE_NAME")
            .displayName("Table Name")
            .description("The name of the table in the database.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAPPING_NAME = new PropertyDescriptor
            .Builder().name("MAPPING_NAME")
            .displayName("Mapping name")
            .description("The name of the mapping responsible for storing the data in the appropriate columns.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor MAX_BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("max-batch-size")
            .displayName("Maximum batch size")
            .description("Maximum count of flow files being processed in one batch.")
            .required(true)
            .addValidator(StandardValidators.NUMBER_VALIDATOR)
            .defaultValue("100")
            .build();

    public static final PropertyDescriptor MY_SERVICE = new PropertyDescriptor
            .Builder().name("MY_SERVICE")
            .displayName("My service")
            .description("Example Service")
            .required(true)
            .identifiesControllerService(MyService.class)
            .build();

    public static final Relationship RL_SUCCEEDED = new Relationship.Builder()
            .name("RL_SUCCEEDED")
            .description("Relationsship for success")
            .build();

    public static final Relationship RL_FAILED = new Relationship.Builder()
            .name("RL_FAILED")
            .description("Relationship for failure")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(MY_PROPERTY);
        descriptors.add(MY_SERVICE);
        descriptors.add(DB_NAME);
        descriptors.add(TABLE_NAME);
        descriptors.add(MAPPING_NAME);
        descriptors.add(MAX_BATCH_SIZE);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(RL_SUCCEEDED);
        relationships.add(RL_FAILED);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }

        MyService service = context.getProperty(MY_SERVICE).asControllerService(MyService.class);

        try (final InputStream in = session.read(flowFile))
        {
            int timeoutInSec = 1000;

            IngestClient client = service.getAdxClient();

            IngestionProperties ingestionProperties = new IngestionProperties(context.getProperty(DB_NAME).getValue(),
                    context.getProperty(TABLE_NAME).getValue());

            ingestionProperties.setIngestionMapping(context.getProperty(MAPPING_NAME).getValue(),
                    IngestionMapping.IngestionMappingKind.Json);

            ingestionProperties.setReportMethod(QueueAndTable);
            ingestionProperties.setReportLevel(IngestionProperties.IngestionReportLevel.FailuresAndSuccesses);
            ingestionProperties.setFlushImmediately(true);

            StreamSourceInfo info = new StreamSourceInfo(in);

            IngestionResult result = client.ingestFromStream(info, ingestionProperties);

            List<IngestionStatus> statuses = result.getIngestionStatusCollection();

            // step 3: poll on the result.
            while (statuses.get(0).status == OperationStatus.Pending && timeoutInSec > 0) {
                Thread.sleep(50);
                timeoutInSec -= 1;
                statuses = result.getIngestionStatusCollection();
            }

            if(statuses.get(0).status == OperationStatus.Succeeded)
            {
                getLogger().info(statuses.get(0).status.toString());
                session.transfer(flowFile, RL_SUCCEEDED);
            }

            if(statuses.get(0).status == OperationStatus.Failed)
            {
                getLogger().error(statuses.get(0).status.toString());
                session.transfer(flowFile, RL_FAILED);
            }
            session.commit();

        } catch (IOException e) {
            throw new ProcessException(e);
        } catch (IngestionClientException | IngestionServiceException | StorageException | URISyntaxException e) {
            e.printStackTrace();
            throw new ProcessException(e);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
