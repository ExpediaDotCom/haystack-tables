package com.expedia.www.haystack.sql.resources;


import com.codahale.metrics.annotation.Timed;
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

    public SqlQuery(final QueryExecutor executor) {
        Validate.notNull(executor);

        this.executor = executor;
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Timed
    public Response submit(final QueryRequest request) throws Exception {
        final QueryResponse response = executor.execute(request);
        return Response.status(response.getHttpStatusCode()).entity(response).build();
    }

    @GET
    @Timed
    public List<QueryMetadata> list() throws Exception {
        return executor.list();
    }

    @DELETE
    @Path("/{view}")
    @Timed
    public Response delete(@PathParam("view") String view) throws Exception {
        QueryResponse response = executor.delete(view);
        return Response.status(response.getHttpStatusCode()).entity(response).build();
    }
}
