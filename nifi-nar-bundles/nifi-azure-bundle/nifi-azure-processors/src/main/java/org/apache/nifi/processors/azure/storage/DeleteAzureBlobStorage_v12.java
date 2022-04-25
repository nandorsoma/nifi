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
package org.apache.nifi.processors.azure.storage;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.DeleteSnapshotsOptionType;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.azure.storage.utils.AzureBlobV12Utils;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Tags({"azure", "microsoft", "cloud", "storage", "blob"})
@SeeAlso({ListAzureBlobStorage_v12.class, FetchAzureBlobStorage_v12.class, PutAzureBlobStorage_v12.class})
@CapabilityDescription("Deletes the specified blob from Azure Blob Storage. The processor uses Azure Blob Storage client library v12.")
@InputRequirement(Requirement.INPUT_REQUIRED)
public class DeleteAzureBlobStorage_v12 extends AbstractProcessor {

    private static final Logger logger = LoggerFactory.getLogger(DeleteAzureBlobStorage_v12.class);

    public static final AllowableValue DELETE_SNAPSHOTS_NONE = new AllowableValue("NONE", "None", "Delete the blob only.");

    public static final AllowableValue DELETE_SNAPSHOTS_ALSO = new AllowableValue(DeleteSnapshotsOptionType.INCLUDE.name(), "Include Snapshots", "Delete the blob and its snapshots.");

    public static final AllowableValue DELETE_SNAPSHOTS_ONLY = new AllowableValue(DeleteSnapshotsOptionType.ONLY.name(), "Delete Snapshots Only", "Delete only the blob's snapshots.");

    public static final PropertyDescriptor DELETE_SNAPSHOTS_OPTION = new PropertyDescriptor.Builder()
            .name("delete-snapshots-option")
            .displayName("Delete Snapshots Option")
            .description("Specifies the snapshot deletion options to be used when deleting a blob.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues(DELETE_SNAPSHOTS_NONE, DELETE_SNAPSHOTS_ALSO, DELETE_SNAPSHOTS_ONLY)
            .defaultValue(DELETE_SNAPSHOTS_NONE.getValue())
            .required(true)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            AzureStorageUtils.STORAGE_CREDENTIALS_SERVICE,
            AzureStorageUtils.CONTAINER,
            AzureBlobV12Utils.BLOB_NAME,
            DELETE_SNAPSHOTS_OPTION,
            AzureStorageUtils.PROXY_CONFIGURATION_SERVICE
    ));

    private static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            AzureBlobV12Utils.REL_SUCCESS,
            AzureBlobV12Utils.REL_FAILURE
    )));

    private BlobServiceClient storageClient;

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        storageClient = AzureBlobV12Utils.createStorageClient(context);
    }

    @OnStopped
    public void onStopped() {
        storageClient = null;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        String containerName = context.getProperty(AzureStorageUtils.CONTAINER).evaluateAttributeExpressions(flowFile).getValue();
        String blobName = context.getProperty(AzureBlobV12Utils.BLOB_NAME).evaluateAttributeExpressions(flowFile).getValue();
        String deleteSnapshotsOption = context.getProperty(DELETE_SNAPSHOTS_OPTION).getValue();

        long startNanos = System.nanoTime();
        try {
            BlobContainerClient containerClient = storageClient.getBlobContainerClient(containerName);
            BlobClient blobClient = containerClient.getBlobClient(blobName);

            String provenanceMesage;
            if (blobClient.exists()) {
                DeleteSnapshotsOptionType deleteSnapshotsOptionType = getDeleteSnapshotsOptionType(deleteSnapshotsOption);
                blobClient.deleteWithResponse(deleteSnapshotsOptionType, null, null, null);
                provenanceMesage = getProvenanceMessage(deleteSnapshotsOptionType);
            } else {
                provenanceMesage = "Blob does not exist, nothing to delete";
            }

            session.transfer(flowFile, AzureBlobV12Utils.REL_SUCCESS);

            long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            session.getProvenanceReporter().invokeRemoteProcess(flowFile, blobClient.getBlobUrl(), String.format("%s (%d ms)", provenanceMesage, transferMillis));
        } catch (Exception e) {
            logger.error("Failed to delete the specified blob ({}) from Azure Blob Storage. Routing to failure", blobName, e);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, AzureBlobV12Utils.REL_FAILURE);
        }
    }

    private DeleteSnapshotsOptionType getDeleteSnapshotsOptionType(String deleteSnapshotOption) {
        try {
            return DeleteSnapshotsOptionType.valueOf(deleteSnapshotOption);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private String getProvenanceMessage(DeleteSnapshotsOptionType deleteSnapshotsOptionType) {
        if (deleteSnapshotsOptionType == null) {
            return "Blob deleted";
        }
        switch (deleteSnapshotsOptionType) {
            case INCLUDE:
                return "Blob deleted along with its snapshots";
            case ONLY:
                return "Blob's snapshots deleted";
            default:
                throw new IllegalArgumentException("Unhandled DeleteSnapshotsOptionType: " + deleteSnapshotsOptionType);
        }
    }
}
