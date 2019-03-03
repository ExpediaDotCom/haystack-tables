package com.expedia.www.haystack.sql.executors;

import com.expedia.www.haystack.sql.entities.QueryMetadata;
import com.expedia.www.haystack.sql.entities.QueryRequest;
import com.expedia.www.haystack.sql.entities.QueryResponse;
import com.google.gson.Gson;
import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.Configuration;
import io.kubernetes.client.apis.AppsV1beta2Api;
import io.kubernetes.client.models.*;
import io.kubernetes.client.util.Config;
import io.kubernetes.client.util.Yaml;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.Validate;
import org.joda.time.DateTime;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import static com.expedia.www.haystack.sql.utils.Utils.isSubset;

@Slf4j
public class K8sQueryExecutor implements QueryExecutor {

    private static String QUERY_ANNOTATION_NAME = "haystack/query";
    private static String LAST_UPDATED_ANNOTATION_NAME = "haystack/lastUpdatedTimestamp";
    private static String K8S_DEPLOYMENT_NAME_PREFIX = "haystack-table-";
    private AppsV1beta2Api apis;
    private String namespace;
    private String labelSelector;
    private int replicas;
    private String image;

    @Override
    public QueryResponse execute(final QueryRequest request) throws Exception {
        final QueryResponse response = new QueryResponse();

        // check if the query is already running with given view name or a similar query already runs with a different name
        final List<QueryMetadata> runningQueries = list();
        for(final QueryMetadata running : runningQueries) {
            // is there is a name conflict?
            if(running.getQuery().getView().equalsIgnoreCase(request.getView())) {
                response.setMessage(String.format("The view with the given name '%s' is already running.", request.getView()));
                response.setHttpStatusCode(409);
                return response;
            } else {
                // is this query a subset of already running query?
                if(isSubset(request, running.getQuery())) {
                    response.setMessage(String.format("A view with name '%s' exists that satisfies the request",
                            running.getQuery().getView()));
                    response.setHttpStatusCode(409);
                    return response;
                }
            }
        }


        try {
            createDeployment(request, response);
            createService();
        } catch (final ApiException ex) {
            // this is to handle a potential race condition where user submits his query more than once
            if (ex.getCode() == 409) {
                response.setHttpStatusCode(200);
                response.setMessage("The requested query is already submitted before and is running.");
            } else {
                response.setHttpStatusCode(ex.getCode());
                response.setMessage(ex.getMessage());
                log.error("Failed to execute the query {}", request, ex);
            }
        }
        return response;
    }

    private void createService() {

    }

    private void createDeployment(final QueryRequest request, final QueryResponse response) throws ApiException {
        final V1beta2Deployment deployment = Yaml.loadAs(new InputStreamReader(
                        this.getClass().getResourceAsStream("/k8s-deployment.yml")),
                V1beta2Deployment.class);

        render(deployment, request);

        apis.createNamespacedDeployment(namespace, deployment, false, "true", "false");
        response.setHttpStatusCode(200);
        response.setViewName(request.getView());
        response.setMessage("The requested query has been submitted successfully!");
    }

    @Override
    public List<QueryMetadata> list() throws Exception {
        final V1beta2DeploymentList deployments = apis.listNamespacedDeployment(
                namespace,
                true,
                null,
                null,
                null,
                labelSelector,
                5000,
                null,
                30,
                false);

        final List<QueryMetadata> result = new ArrayList<>();
        for (final V1beta2Deployment deployment : deployments.getItems()) {
            final Gson gson = new Gson();
            final QueryMetadata m = new QueryMetadata();

            deployment.getMetadata().getAnnotations().forEach((key, value) -> {
                if(key.equalsIgnoreCase(LAST_UPDATED_ANNOTATION_NAME)) {
                    m.setLastUpdatedTimestamp(new DateTime(Long.parseLong(value)));
                } else if (key.equalsIgnoreCase(QUERY_ANNOTATION_NAME)) {
                    m.setQuery(gson.fromJson(value, QueryRequest.class));
                }
            });

            m.setCreateTimestamp(deployment.getMetadata().getCreationTimestamp());
            m.setRunning(Objects.equals(deployment.getStatus().getReplicas(), deployment.getStatus().getReadyReplicas()));
            result.add(m);
        }

        return result;
    }

    @Override
    public QueryResponse delete(String viewName) throws Exception {
        final QueryResponse response = new QueryResponse();
        final V1DeleteOptions options = new V1DeleteOptions();
        try {
            response.setViewName(viewName);
            apis.deleteNamespacedDeployment(K8S_DEPLOYMENT_NAME_PREFIX + viewName,
                    namespace,
                    options,
                    null,
                    "false",
                    45,
                    false,
                    null);
            response.setHttpStatusCode(200);
            response.setMessage("successfully delete !!");
        } catch (ApiException ex) {
            log.error("Failed to delete the deployment with name{}", viewName, ex);
            response.setMessage(ex.getMessage());
            response.setHttpStatusCode(ex.getCode());
        }
        return response;
    }

    @Override
    public String name() {
        return "k8s";
    }

    public void init(final Map<String, String> properties) throws IOException {
        this.namespace = properties.getOrDefault("namespace", "");
        this.labelSelector = properties.getOrDefault("labelselector", "");
        this.replicas = Integer.parseInt(properties.getOrDefault("replicas", "1"));
        this.image = properties.getOrDefault("image", "");

        Validate.notEmpty(this.namespace);
        Validate.notEmpty(this.labelSelector);

        final ApiClient client = Config.defaultClient();
        Configuration.setDefaultApiClient(client);
        this.apis = new AppsV1beta2Api();
        apis.setApiClient(client);
    }

    private void render(final V1beta2Deployment body, final QueryRequest request) {
        body.getMetadata().setName(K8S_DEPLOYMENT_NAME_PREFIX + request.getView());

        final V1beta2DeploymentSpec spec = body.getSpec();
        spec.replicas(replicas);
        for (final V1Container container : spec.getTemplate().getSpec().getContainers()) {
            container.setName(request.getView());
            container.setImage(this.image);
            addEnvVars(container, request);
        }
        addAnnotations(body, request);
    }

    private void addEnvVars(final V1Container container, final QueryRequest query) {
        final List<V1EnvVar> envVars = container.getEnv() == null ? new ArrayList<>() : container.getEnv();
        envVars.add(new V1EnvVar().name("HAYSTACK_PROP_SQL_QUERY").value(new Gson().toJson(query)));
        System.getenv().forEach((key, value) -> {
            if (key.startsWith("HAYSTACK_PROP_KAFKA_") || key.startsWith("HAYSTACK_PROP_S3_")) {
                envVars.add(new V1EnvVar().name(key).value(value));
            }
        });
        container.setEnv(envVars);
    }

    private void addAnnotations(final V1beta2Deployment deployment, final QueryRequest query) {
        Map<String, String> annotations = deployment.getMetadata().getAnnotations();
        if (annotations == null) {
            annotations = new HashMap<>();
            deployment.getMetadata().setAnnotations(annotations);
        }
        annotations.put(QUERY_ANNOTATION_NAME, new Gson().toJson(query));
        annotations.put(LAST_UPDATED_ANNOTATION_NAME, Long.toString(new Date().getTime()));
    }
}
