package com.viasat.burroughs;

import com.viasat.burroughs.service.StatusService;
import com.viasat.burroughs.service.model.HealthStatus;
import jdk.jshell.Snippet;
import org.junit.Assert;
import org.junit.Test;

public class HealthTest {

    @Test
    public void checkServerHealth() {
        StatusService service = new StatusService("http://localhost:8088");
        HealthStatus status = service.checkConnection();
        Assert.assertNotNull(status);
        Assert.assertTrue(status.isHealthy());
        Assert.assertNotNull(status.getDetails());
    }
}
