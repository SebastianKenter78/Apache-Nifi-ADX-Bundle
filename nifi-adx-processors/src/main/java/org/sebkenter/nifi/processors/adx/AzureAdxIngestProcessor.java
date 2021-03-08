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
package org.sebkenter.nifi.processors.adx;

import org.sebkenter.nifi.adx.IAzureAdxConnectionService;
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
import org.apache.nifi.components.AllowableValue;
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

import java.io.*;
import java.net.URISyntaxException;
import java.util.*;

import static com.microsoft.azure.kusto.ingest.IngestionProperties.IngestionReportMethod.QueueAndTable;
import static com.microsoft.azure.kusto.ingest.IngestionProperties.IngestionReportMethod.Table;

@Tags({"azure", "adx"})
@CapabilityDescription("The Azure ADX Processor sends FlowFiles using the ADX-Service to the provided Azure Data" +
        "Explorer Ingest Endpoint.")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class AzureAdxIngestProcessor extends AbstractProcessor {

    public static final AllowableValue AVRO = new AllowableValue(
            "AVRO", ".avro",
            "A legacy implementation for the Avro container file. The following codes are supported: " +
                    "null, deflate. (For snappy, use the apacheavro file format).");

    public static final AllowableValue APACHEAVRO = new AllowableValue(
            "APACHEAVRO", ".avro",
            "An Avro format with support for logical types and for the snappy compression codec.");

    public static final AllowableValue CSV = new AllowableValue(
            "CSV", ".csv",
            "A text file with comma-separated values (,). For more information, see RFC 4180: Common Format " +
                    "and MIME Type for Comma-Separated Values (CSV) Files.");

    public static final AllowableValue JSON = new AllowableValue(
            "JSON", ".json",
            "A text file containing JSON objects separated by \\n or \\r\\n. For more information, " +
                    "see JSON Lines (JSONL).");

    public static final AllowableValue MULTIJSON = new AllowableValue(
            "MULTIJSON", ".multijson",
            "A text file containing a JSON array of property containers (each representing a record) or any " +
                    "number of property containers separated by spaces, \\n or \\r\\n. Each property container may be " +
                    "spread across multiple lines. This format is preferable to JSON unless the data is not property " +
                    "containers.");

    public static final AllowableValue ORC = new AllowableValue(
            "ORC",".orc","An ORC file.");

    public static final AllowableValue PARQUET = new AllowableValue(
            "PARQUET",".parquet","A parquet file.");

    public static final AllowableValue PSV = new AllowableValue(
            "PSV",".psv","A text file with values separated by vertical bars (|).");

    public static final AllowableValue SCSV = new AllowableValue(
            "SCSV",".scsv","A text file with values separated by semicolons (;).");

    public static final AllowableValue SOHSV = new AllowableValue(
            "SOHSV",".sohsv",
            "A text file with SOH-separated values. (SOH is the ASCII code point 1. " +
                    "This format is used by Hive in HDInsight).");

    public static final AllowableValue TSV = new AllowableValue(
            "TSV",".tsv","A text file with tab delimited values (\\t).");

    public static final AllowableValue TSVE = new AllowableValue(
            "TSVE",".tsv",
            "A text file with tab-delimited values (\\t). A backslash (\\) is used as escape character.");

    public static final AllowableValue TXT = new AllowableValue(
            "TXT",".txt",
            "A text file with lines separated by \\n. Empty lines are skipped.");

    public static final AllowableValue IRL_NONE = new AllowableValue(
            "IRL_NONE", "IngestionReportLevel:None",
            "No reports are generated at all.");

    public static final AllowableValue IRL_FAIL = new AllowableValue(
            "IRL_FAIL", "IngestionReportLevel:Failure",
            "Status get's reported on failure only.");

    public static final AllowableValue IRL_FAS = new AllowableValue(
            "IRL_FAS", "IngestionReportLevel:FailureAndSuccess",
            "Status get's reported on failure and success.");

    public static final AllowableValue IRM_QUEUE = new AllowableValue(
            "IRM_QUEUE", "IngestionReportMethod:Queue",
            "Reports are generated for queue-events.");

    public static final AllowableValue IRM_TABLE = new AllowableValue(
            "IRM_TABLE", "IngestionReportMethod:Table",
            "Reports are generated for table-events.");

    public static final AllowableValue IRM_TABLEANDQUEUE = new AllowableValue(
            "IRM_TABLEANDQUEUE", "IngestionReportMethod:TableAndQueue",
            "Reports are generated for table- and queue-events.");

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

    static final PropertyDescriptor FLUSH_IMMEDIATE = new PropertyDescriptor.Builder()
            .name("FLUSH_IMMEDIATE")
            .displayName("Flush immediate")
            .description("Flush the content sent immediately to the ingest entpoint.")
            .required(true)
            .defaultValue("false")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor DATA_FORMAT = new PropertyDescriptor.Builder()
            .name("DATA_FORMAT")
            .displayName("Data format")
            .description("The format of the data that is sent to ADX.")
            .required(true)
            .allowableValues(AVRO, APACHEAVRO, CSV, JSON, MULTIJSON, ORC, PARQUET, PSV, SCSV, SOHSV, TSV, TSVE, TXT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor IR_LEVEL = new PropertyDescriptor.Builder()
            .name("IR_LEVEL")
            .displayName("IngestionReportLevel")
            .description("ADX can report events on several levels: None, Failure and Failure&Success.")
            .required(true)
            .allowableValues(IRL_NONE, IRL_FAIL, IRL_FAS)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor IR_METHOD = new PropertyDescriptor.Builder()
            .name("IR_METHOD")
            .displayName("IngestionReportMethod")
            .description("ADX can report events on several methods: Table, Queue and Table&Queue.")
            .required(true)
            .allowableValues(IRM_TABLE, IRM_QUEUE, IRM_TABLEANDQUEUE)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor ADX_SERVICE = new PropertyDescriptor
            .Builder().name("ADX_SERVICE")
            .displayName("AzureADXConnectionService")
            .description("Service that provides the ADX-Connections.")
            .required(true)
            .identifiesControllerService(IAzureAdxConnectionService.class)
            .build();

    public static final Relationship RL_SUCCEEDED = new Relationship.Builder()
            .name("RL_SUCCEEDED")
            .description("Relationship for success")
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
        descriptors.add(ADX_SERVICE);
        descriptors.add(DB_NAME);
        descriptors.add(TABLE_NAME);
        descriptors.add(MAPPING_NAME);
        descriptors.add(FLUSH_IMMEDIATE);
        descriptors.add(DATA_FORMAT);
        descriptors.add(IR_LEVEL);
        descriptors.add(IR_METHOD);
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

        IAzureAdxConnectionService service = context.getProperty(ADX_SERVICE).asControllerService(IAzureAdxConnectionService.class);

        try (final InputStream in = session.read(flowFile))
        {
            IngestClient client = service.getAdxClient();

            IngestionProperties ingestionProperties = new IngestionProperties(context.getProperty(DB_NAME).getValue(),
                    context.getProperty(TABLE_NAME).getValue());

            ingestionProperties.setIngestionMapping(context.getProperty(MAPPING_NAME).getValue(),
                    IngestionMapping.IngestionMappingKind.Json);

            switch(context.getProperty(DATA_FORMAT).getValue()) {
                case "AVRO": ingestionProperties.setDataFormat(IngestionProperties.DATA_FORMAT.avro);
                case "APACHEAVRO": ingestionProperties.setDataFormat(IngestionProperties.DATA_FORMAT.apacheavro);
                case "CSV": ingestionProperties.setDataFormat(IngestionProperties.DATA_FORMAT.csv);
                case "JSON": ingestionProperties.setDataFormat(IngestionProperties.DATA_FORMAT.json);
                case "MULTIJSON": ingestionProperties.setDataFormat(IngestionProperties.DATA_FORMAT.multijson);
                case "ORC": ingestionProperties.setDataFormat(IngestionProperties.DATA_FORMAT.orc);
                case "PARQUET": ingestionProperties.setDataFormat(IngestionProperties.DATA_FORMAT.parquet);
                case "PSV": ingestionProperties.setDataFormat(IngestionProperties.DATA_FORMAT.psv);
                case "SCSV": ingestionProperties.setDataFormat(IngestionProperties.DATA_FORMAT.scsv);
                case "SOHSV": ingestionProperties.setDataFormat(IngestionProperties.DATA_FORMAT.sohsv);
                case "TSV": ingestionProperties.setDataFormat(IngestionProperties.DATA_FORMAT.tsv);
                case "TSVE": ingestionProperties.setDataFormat(IngestionProperties.DATA_FORMAT.tsve);
                case "TXT": ingestionProperties.setDataFormat(IngestionProperties.DATA_FORMAT.txt);
            }

            switch(context.getProperty(IR_LEVEL).getValue()) {
                case "IRL_NONE": ingestionProperties.setReportLevel(IngestionProperties.IngestionReportLevel.None);
                case "IRL_FAIL": ingestionProperties.setReportLevel(IngestionProperties.IngestionReportLevel.FailuresOnly);
                case "IRL_FAS": ingestionProperties.setReportLevel(IngestionProperties.IngestionReportLevel.FailuresAndSuccesses);
            }

            switch (context.getProperty(IR_METHOD).getValue()) {
                case "IRM_TABLE": ingestionProperties.setReportMethod(Table);
                case "IRM_QUEUE": ingestionProperties.setReportMethod(IngestionProperties.IngestionReportMethod.Queue);
                case "IRM_TABLEANDQUEUE": ingestionProperties.setReportMethod(QueueAndTable);
            }

            if (context.getProperty(FLUSH_IMMEDIATE).getValue().equals("true"))
            {
                ingestionProperties.setFlushImmediately(true);
            }
            else {
                ingestionProperties.setFlushImmediately(false);
            }

            getLogger().info("Ingesting with: " + ingestionProperties.toString());

            StreamSourceInfo info = new StreamSourceInfo(in);

            IngestionResult result = client.ingestFromStream(info, ingestionProperties);

            List<IngestionStatus> statuses = result.getIngestionStatusCollection();

            while (statuses.get(0).status == OperationStatus.Pending) {
                Thread.sleep(50);
                statuses = result.getIngestionStatusCollection();
            }

            getLogger().info("Operation status: " + statuses.get(0).status);

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
