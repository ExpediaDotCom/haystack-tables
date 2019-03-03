package com.expedia.www.haystack.sql.resources;


import com.codahale.metrics.annotation.Timed;
import com.expedia.www.haystack.sql.entities.QueryMetadata;
import com.expedia.www.haystack.sql.entities.QueryResponse;
import com.expedia.www.haystack.sql.executors.QueryExecutor;
import com.expedia.www.haystack.table.entities.Query;
import org.apache.commons.lang3.Validate;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

@Path("/")
@Produces(MediaType.APPLICATION_JSON)
public class Views {
    private final QueryExecutor executor;

    public Views(final QueryExecutor executor) {
        Validate.notNull(executor);

        this.executor = executor;
    }

    @POST
    @Path("/view")
    @Consumes(MediaType.APPLICATION_JSON)
    @Timed
    public Response submit(final Query request) throws Exception {
        final QueryResponse response = executor.execute(request);
        return Response.status(response.getHttpStatusCode()).entity(response).build();
    }

    @GET
    @Path("/views")
    @Timed
    public List<QueryMetadata> list() throws Exception {
        return executor.list();
    }

    @DELETE
    @Path("/view/{viewName}")
    @Timed
    public Response delete(@PathParam("viewName") String viewName) throws Exception {
        QueryResponse response = executor.delete(viewName);
        return Response.status(response.getHttpStatusCode()).entity(response).build();
    }
}
