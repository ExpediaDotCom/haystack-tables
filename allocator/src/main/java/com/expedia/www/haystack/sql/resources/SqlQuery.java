package com.expedia.www.haystack.sql.resources;


import com.codahale.metrics.annotation.Timed;
import com.expedia.www.haystack.sql.AthenaRefreshJob;
import com.expedia.www.haystack.sql.entities.QueryMetadata;
import com.expedia.www.haystack.sql.entities.QueryRequest;
import com.expedia.www.haystack.sql.entities.QueryResponse;
import com.expedia.www.haystack.sql.executors.QueryExecutor;
import org.apache.commons.lang3.Validate;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

@Path("/sql")
@Produces(MediaType.APPLICATION_JSON)
public class SqlQuery {
    private final QueryExecutor executor;
    private final AthenaRefreshJob refreshJob;

    public SqlQuery(final QueryExecutor executor, final AthenaRefreshJob refreshJob) {
        Validate.notNull(executor);
        Validate.notNull(refreshJob);

        this.executor = executor;
        this.refreshJob = refreshJob;
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Timed
    public Response submitQuery(final QueryRequest request) throws Exception {
        final QueryResponse response = executor.execute(request);
        return Response.status(response.getHttpStatusCode()).entity(response).build();
    }

    @GET
    @Timed
    public List<QueryMetadata> list() throws Exception {
        return executor.list();
    }

    @DELETE
    @Path("/{executionId}")
    @Timed
    public Response delete(@PathParam("executionId") String executionId) throws Exception {
        QueryResponse response = executor.delete(executionId);
        return Response.status(response.getHttpStatusCode()).entity(response).build();
    }

    @POST
    @Path("/athena/refresh")
    @Timed
    public Response refreshAthena() throws Exception {
        executor.list().forEach(refreshJob::refresh);
        return Response.ok().build();
    }
}
