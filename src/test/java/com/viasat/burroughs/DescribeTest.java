package com.viasat.burroughs;

import com.viasat.burroughs.service.model.description.ConnectorDescription;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class DescribeTest extends ServiceTest {

    @Test
    public void testDescribeConnector() {
        ConnectorDescription response = service.executeStatement("DESCRIBE CONNECTOR BURR_CONNECT_E394ABF2CDD8439BB0EB804C92F8C36F;",
                "describe connector");
        assertNotNull(response);
    }
}
