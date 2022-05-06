package org.apache.nifi.processors.azure.storage;

import com.azure.core.credential.AzureSasCredential;
import com.azure.core.credential.TokenCredential;
import com.azure.core.cryptography.AsyncKeyEncryptionKey;
import com.azure.core.http.HttpClient;
import com.azure.core.http.netty.NettyAsyncHttpClientBuilder;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.ManagedIdentityCredentialBuilder;
import com.azure.security.keyvault.keys.cryptography.KeyEncryptionKeyClientBuilder;
import com.azure.security.keyvault.keys.cryptography.models.KeyWrapAlgorithm;
import com.azure.security.keyvault.keys.models.JsonWebKey;
import com.azure.security.keyvault.keys.models.KeyOperation;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.specialized.cryptography.EncryptedBlobClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.processors.azure.AbstractAzureBlobProcessor_v12;
import org.apache.nifi.processors.azure.storage.utils.AzureBlobClientSideEncryptionMethod;
import org.apache.nifi.processors.azure.storage.utils.AzureBlobClientSideEncryptionUtils;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;
import org.apache.nifi.services.azure.storage.AzureStorageCredentialsDetails_v12;
import org.apache.nifi.services.azure.storage.AzureStorageCredentialsService_v12;
import reactor.core.publisher.Mono;

import javax.crypto.spec.SecretKeySpec;
import java.util.Arrays;

public class BlobContainerClientFactory {

    private static final String KEY_WRAP_ALGORITHM = "AES";

    private BlobContainerClient blobContainerClient;

    public BlobContainerClient getClient(PropertyContext context, String containerName, String blobName) {
        final String cseKeyTypeValue = context.getProperty(AzureBlobClientSideEncryptionUtils.CSE_KEY_TYPE).getValue();
        final AzureBlobClientSideEncryptionMethod cseKeyType = AzureBlobClientSideEncryptionMethod.valueOf(cseKeyTypeValue);

        if (AzureBlobClientSideEncryptionMethod.NONE == cseKeyType) {
            if (blobContainerClient == null) {
                blobContainerClient = createStandardClient(context).getBlobContainerClient(containerName);
            }
            return blobContainerClient;
        } else {
            return createSecureClient(context, containerName, blobName);
        }
    }

    public static BlobServiceClient createStandardClient(PropertyContext context) {
        final AzureStorageCredentialsService_v12 credentialsService = context.getProperty(AbstractAzureBlobProcessor_v12.STORAGE_CREDENTIALS_SERVICE).asControllerService(AzureStorageCredentialsService_v12.class);
        final AzureStorageCredentialsDetails_v12 credentialsDetails = credentialsService.getCredentialsDetails();

        final BlobServiceClientBuilder clientBuilder = new BlobServiceClientBuilder();
        clientBuilder.endpoint(String.format("https://%s.%s", credentialsDetails.getAccountName(), credentialsDetails.getEndpointSuffix()));

        final NettyAsyncHttpClientBuilder nettyClientBuilder = new NettyAsyncHttpClientBuilder();
        AzureStorageUtils.configureProxy(nettyClientBuilder, context);

        final HttpClient nettyClient = nettyClientBuilder.build();
        clientBuilder.httpClient(nettyClient);

        configureCredential(clientBuilder, credentialsDetails);

        return clientBuilder.buildClient();
    }

    public static BlobContainerClient createSecureClient(PropertyContext context, String containerName, String blobName) {
        final AzureStorageCredentialsService_v12 credentialsService = context.getProperty(AbstractAzureBlobProcessor_v12.STORAGE_CREDENTIALS_SERVICE).asControllerService(AzureStorageCredentialsService_v12.class);
        final AzureStorageCredentialsDetails_v12 credentialsDetails = credentialsService.getCredentialsDetails();

        final EncryptedBlobClientBuilder clientBuilder = new EncryptedBlobClientBuilder();
        clientBuilder.endpoint(String.format("https://%s.%s", credentialsDetails.getAccountName(), credentialsDetails.getEndpointSuffix()));
        clientBuilder.containerName(containerName);

        final NettyAsyncHttpClientBuilder nettyClientBuilder = new NettyAsyncHttpClientBuilder();
        AzureStorageUtils.configureProxy(nettyClientBuilder, context);

        final HttpClient nettyClient = nettyClientBuilder.build();
        clientBuilder.httpClient(nettyClient);

        configureCredential(clientBuilder, credentialsDetails);

        final String cseKeyId = context.getProperty(AzureBlobClientSideEncryptionUtils.CSE_KEY_ID).getValue();

        final String cseSymmetricKeyHex = context.getProperty(AzureBlobClientSideEncryptionUtils.CSE_SYMMETRIC_KEY_HEX).getValue();

        try {
            clientBuilder.key(createKeyEncryptionKey(cseKeyId, cseSymmetricKeyHex), KeyWrapAlgorithm.A256KW.toString());
        } catch (DecoderException e) {
            throw new RuntimeException("Unable to set key in EncryptedBlobClientBuilder.", e);
        }
        clientBuilder.requiresEncryption(true);

        clientBuilder.blobName(blobName);

        return clientBuilder.buildEncryptedBlobClient().getContainerClient();
    }

    private static void configureCredential(BlobServiceClientBuilder clientBuilder, AzureStorageCredentialsDetails_v12 credentialsDetails) {
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
                TokenCredential credential = tokenRequestContext -> Mono.just(credentialsDetails.getAccessToken());
                clientBuilder.credential(credential);
                break;
            default:
                throw new IllegalArgumentException("Unhandled credentials type: " + credentialsDetails.getCredentialsType());
        }
    }

    private static void configureCredential(EncryptedBlobClientBuilder clientBuilder, AzureStorageCredentialsDetails_v12 credentialsDetails) {
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
                TokenCredential credential = tokenRequestContext -> Mono.just(credentialsDetails.getAccessToken());
                clientBuilder.credential(credential);
                break;
            default:
                throw new IllegalArgumentException("Unhandled credentials type: " + credentialsDetails.getCredentialsType());
        }
    }

    private static AsyncKeyEncryptionKey createKeyEncryptionKey(String keyId, String cseSymmetricKeyHex) throws DecoderException {
        byte[] keyBytes = Hex.decodeHex(cseSymmetricKeyHex.toCharArray());

        JsonWebKey localKey = JsonWebKey
                .fromAes(new SecretKeySpec(keyBytes, KeyWrapAlgorithm.A256KW.toString()), Arrays.asList(KeyOperation.WRAP_KEY, KeyOperation.UNWRAP_KEY))
                .setId(keyId);

        //TODO: block?
        AsyncKeyEncryptionKey keyEncryptionKey = new KeyEncryptionKeyClientBuilder().buildAsyncKeyEncryptionKey(localKey).block();

        return keyEncryptionKey;
    }
}
