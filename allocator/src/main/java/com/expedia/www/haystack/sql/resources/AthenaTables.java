package com.expedia.www.haystack.sql.resources;


import com.codahale.metrics.annotation.Timed;
import com.expedia.www.haystack.sql.AthenaRefreshJob;
import com.expedia.www.haystack.sql.executors.QueryExecutor;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;

@Path("/athena")
public class AthenaTables {

    private final QueryExecutor executor;
    private final AthenaRefreshJob refreshJob;

    public AthenaTables(final QueryExecutor executor, final AthenaRefreshJob refreshJob) {
        this.executor = executor;
        this.refreshJob = refreshJob;
    }

    @POST
    @Path("/refresh")
    @Timed
    public Response refreshAthena() throws Exception {
        executor.list().forEach(refreshJob::refresh);
        return Response.ok().build();
    }
}

