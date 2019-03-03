package com.expedia.www.haystack.writer.resources;

import com.expedia.www.haystack.writer.task.TaskStateListener;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.concurrent.atomic.AtomicBoolean;

@Path("/isActive")
public class HealthCheck implements TaskStateListener{
    private AtomicBoolean isActive = new AtomicBoolean(true);

    @GET
    public Response isHealthy() {
        if (isActive.get()) {
            return Response.ok("ACTIVE", MediaType.TEXT_PLAIN).build();
        } else {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        }
    }

    @Override
    public void onChange(State state) {
        if (state == State.FAILED) {
            isActive.set(false);
        }
    }
}
