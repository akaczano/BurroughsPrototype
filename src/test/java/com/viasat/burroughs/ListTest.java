package com.viasat.burroughs;

import com.viasat.burroughs.service.StatementService;
import com.viasat.burroughs.service.model.StatementError;
import com.viasat.burroughs.service.model.StatementResponse;
import com.viasat.burroughs.service.model.list.ListResponse;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class ListTest extends ServiceTest {

    @Test
    public void testListStreams() {
        Object response = service.executeStatement("LIST topics");
        assertNotNull(response);
        Assert.assertTrue(response instanceof StatementError);

        Object response2 = service.executeStatement("LIST STREAMS;");
        assertNotNull(response);
        Assert.assertTrue(response2 instanceof ListResponse);
        ListResponse listResponse = (ListResponse) response2;
        assertNotNull(listResponse.getStreams());
        Assert.assertTrue(listResponse.getStreams().length > 0);
    }

    @Test
    public void testDescribe() {
        StatementResponse response = service.executeStatement("DESCRIBE SUMS;");
        assertNotNull(response);
    }
}
