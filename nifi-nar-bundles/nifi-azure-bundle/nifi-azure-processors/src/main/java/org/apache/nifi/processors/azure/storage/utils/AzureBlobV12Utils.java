package org.apache.nifi.processors.azure.storage.utils;

import com.azure.core.credential.AzureSasCredential;
import com.azure.core.credential.TokenCredential;
import com.azure.core.http.HttpClient;
import com.azure.core.http.netty.NettyAsyncHttpClientBuilder;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.ManagedIdentityCredentialBuilder;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.common.StorageSharedKeyCredential;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.services.azure.storage.AzureStorageCredentialsDetails_v12;
import org.apache.nifi.services.azure.storage.AzureStorageCredentialsService_v12;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.Map;

import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_BLOBNAME;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_BLOBTYPE;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_CONTAINER;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_ETAG;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_LANG;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_LENGTH;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_MIME_TYPE;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_PRIMARY_URI;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_TIMESTAMP;

public class AzureBlobV12Utils {

    public static final PropertyDescriptor STORAGE_CREDENTIALS_SERVICE = new PropertyDescriptor.Builder()
            .name("storage-credentials-service")
            .displayName("Storage Credentials")
            .description("Controller Service used to obtain Azure Blob Storage Credentials.")
            .identifiesControllerService(AzureStorageCredentialsService_v12.class)
            .required(true)
            .build();

    public static final PropertyDescriptor BLOB_NAME = new PropertyDescriptor.Builder()
            .name("blob-name")
            .displayName("Blob Name")
            .description("The full name of the blob")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All successfully processed FlowFiles are routed to this relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Unsuccessful operations will be transferred to the failure relationship.")
            .build();

    public static BlobServiceClient createStorageClient(PropertyContext context) {
        final AzureStorageCredentialsService_v12 credentialsService = context.getProperty(STORAGE_CREDENTIALS_SERVICE).asControllerService(AzureStorageCredentialsService_v12.class);
        final AzureStorageCredentialsDetails_v12 credentialsDetails = credentialsService.getCredentialsDetails();

        final BlobServiceClientBuilder clientBuilder = new BlobServiceClientBuilder();
        clientBuilder.endpoint(String.format("https://%s.%s", credentialsDetails.getAccountName(), credentialsDetails.getEndpointSuffix()));

        final NettyAsyncHttpClientBuilder nettyClientBuilder = new NettyAsyncHttpClientBuilder();
        AzureStorageUtils.configureProxy(nettyClientBuilder, context);

        final HttpClient nettyClient = nettyClientBuilder.build();
        clientBuilder.httpClient(nettyClient);

        configureCredential(clientBuilder, credentialsService, credentialsDetails);

        return clientBuilder.buildClient();
    }

    private static void configureCredential(BlobServiceClientBuilder clientBuilder, AzureStorageCredentialsService_v12 credentialsService,
                                            AzureStorageCredentialsDetails_v12 credentialsDetails) {
        switch (credentialsDetails.getCredentialsType()) {
            case ACCOUNT_KEY:
                clientBuilder.credential(new StorageSharedKeyCredential(credentialsDetails.getAccountName(), credentialsDetails.getAccountKey()));
                break;
            case SAS_TOKEN:
                clientBuilder.credential(new AzureSasCredential(credentialsDetails.getSasToken()));
                break;
            case MANAGED_IDENTITY:
                clientBuilder.credential(new ManagedIdentityCredentialBuilder()
                        .clientId(credentialsDetails.getManagedIdentityClientId())
                        .build());
                break;
            case SERVICE_PRINCIPAL:
                clientBuilder.credential(new ClientSecretCredentialBuilder()
                        .tenantId(credentialsDetails.getServicePrincipalTenantId())
                        .clientId(credentialsDetails.getServicePrincipalClientId())
                        .clientSecret(credentialsDetails.getServicePrincipalClientSecret())
                        .build());
                break;
            case ACCESS_TOKEN:
                TokenCredential credential = tokenRequestContext -> Mono.just(credentialsService.getCredentialsDetails().getAccessToken());
                clientBuilder.credential(credential);
                break;
            default:
                throw new IllegalArgumentException("Unhandled credentials type: " + credentialsDetails.getCredentialsType());
        }
    }

    public static Map<String, String> createBlobAttributesMap(BlobClient blobClient) {
        Map<String, String> attributes = new HashMap<>();

        BlobProperties properties = blobClient.getProperties();
        String primaryUri = String.format("%s/%s", blobClient.getContainerClient().getBlobContainerUrl(), blobClient.getBlobName());

        attributes.put(ATTR_NAME_CONTAINER, blobClient.getContainerName());
        attributes.put(ATTR_NAME_BLOBNAME, blobClient.getBlobName());
        attributes.put(ATTR_NAME_PRIMARY_URI, primaryUri);
        attributes.put(ATTR_NAME_ETAG, properties.getETag());
        attributes.put(ATTR_NAME_BLOBTYPE, properties.getBlobType().toString());
        attributes.put(ATTR_NAME_MIME_TYPE, properties.getContentType());
        attributes.put(ATTR_NAME_LANG, properties.getContentLanguage());
        attributes.put(ATTR_NAME_TIMESTAMP, String.valueOf(properties.getLastModified()));
        attributes.put(ATTR_NAME_LENGTH, String.valueOf(properties.getBlobSize()));

        return attributes;
    }
}
