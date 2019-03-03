package com.expedia.www.haystack.sql.resources;


import com.codahale.metrics.annotation.Timed;
import com.expedia.www.haystack.sql.AthenaManager;
import com.expedia.www.haystack.sql.executors.QueryExecutor;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;

@Path("/athena")
public class AthenaTables {

    private final QueryExecutor executor;
    private final AthenaManager manager;

    public AthenaTables(final QueryExecutor executor, final AthenaManager manager) {
        this.executor = executor;
        this.manager = manager;
    }

    @POST
    @Path("/refresh")
    @Timed
    public Response refreshAthena() throws Exception {
        executor.list().forEach(manager::refresh);
        return Response.ok().build();
    }
}

