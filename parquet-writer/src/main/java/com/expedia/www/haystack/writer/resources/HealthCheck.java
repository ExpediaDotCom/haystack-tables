package com.expedia.www.haystack.writer.resources;

import com.expedia.www.haystack.writer.task.TaskStateListener;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

@Path("/isActive")
public class HealthCheck implements TaskStateListener{

    volatile boolean isActive = true;

    @GET
    public String isHealthy() {
        return isActive ? "ACTIVE" : "INACTIVE";
    }

    @Override
    public void onChange(State state) {
        if (state == State.FAILED) {
            isActive = false;
        }
    }
}
