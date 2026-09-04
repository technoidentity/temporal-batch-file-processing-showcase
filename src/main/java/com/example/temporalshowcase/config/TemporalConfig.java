package com.example.temporalshowcase.config;

import io.grpc.Metadata;
import io.temporal.authorization.AuthorizationGrpcMetadataProvider;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowClientOptions;
import io.temporal.serviceclient.GrpcMetadataProvider;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import io.temporal.worker.WorkerFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Builds the Temporal client from {@link TemporalProperties}. Supports local
 * (plaintext) and Temporal Cloud (TLS + API key) purely via configuration.
 *
 * The stubs are created lazily ({@code newServiceStubs}), so the application and
 * its workers start even when Temporal is unreachable — the workers reconnect in
 * the background once Temporal is available.
 */
@Slf4j
@Configuration
@EnableConfigurationProperties(TemporalProperties.class)
public class TemporalConfig {

    @Bean
    public WorkflowServiceStubs workflowServiceStubs(TemporalProperties props) {
        TemporalProperties.Connection c = props.getConnection();

        WorkflowServiceStubsOptions.Builder b = WorkflowServiceStubsOptions.newBuilder()
                .setTarget(c.getTarget());

        boolean cloud = c.getApiKey() != null && !c.getApiKey().isBlank();
        if (c.isTls() || cloud) {
            b.setEnableHttps(true);
        }
        if (cloud) {
            // Authorization: Bearer <apiKey>
            final String key = c.getApiKey();
            b.addGrpcMetadataProvider(new AuthorizationGrpcMetadataProvider(() -> "Bearer " + key));
            // temporal-namespace header — required to route API-key calls at the Cloud endpoint
            final String ns = c.getNamespace();
            GrpcMetadataProvider nsHeader = () -> {
                Metadata md = new Metadata();
                md.put(Metadata.Key.of("temporal-namespace", Metadata.ASCII_STRING_MARSHALLER), ns);
                return md;
            };
            b.addGrpcMetadataProvider(nsHeader);
            log.info("Temporal client configured for Cloud (TLS + API key), target={}, namespace={}",
                    c.getTarget(), c.getNamespace());
        } else {
            log.info("Temporal client configured for local/self-hosted, target={}, namespace={}, tls={}",
                    c.getTarget(), c.getNamespace(), c.isTls());
        }

        // Lazy stubs: do not block startup if Temporal is down.
        return WorkflowServiceStubs.newServiceStubs(b.build());
    }

    @Bean
    public WorkflowClient workflowClient(WorkflowServiceStubs stubs, TemporalProperties props) {
        WorkflowClientOptions.Builder opts = WorkflowClientOptions.newBuilder()
                .setNamespace(props.getConnection().getNamespace());
        if (props.getConnection().getIdentity() != null && !props.getConnection().getIdentity().isBlank()) {
            opts.setIdentity(props.getConnection().getIdentity());
        }
        return WorkflowClient.newInstance(stubs, opts.build());
    }

    @Bean
    public WorkerFactory workerFactory(WorkflowClient workflowClient) {
        return WorkerFactory.newInstance(workflowClient);
    }
}
