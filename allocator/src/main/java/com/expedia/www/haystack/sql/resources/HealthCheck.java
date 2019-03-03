package com.expedia.www.haystack.sql.resources;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

@Path("/isActive")
public class HealthCheck {
    @GET
    public String isHealthy() {
        return "ACTIVE";
    }
}
