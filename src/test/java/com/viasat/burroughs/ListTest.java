package com.viasat.burroughs;

import com.viasat.burroughs.service.model.StatementError;
import com.viasat.burroughs.service.model.list.ListResponse;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ListTest extends ServiceTest {

    @Test
    public void testLists() {
        Object response = service.executeStatement("LIST topics");
        assertNotNull(response);
        Assert.assertTrue(response instanceof StatementError);

        Object response2 = service.executeStatement("LIST STREAMS;");
        assertNotNull(response2);
        Assert.assertTrue(response2 instanceof ListResponse);
        ListResponse listResponse = (ListResponse) response2;
        assertNotNull(listResponse.getStreams());


        Object response3 = service.executeStatement("LIST TOpics;");
        assertNotNull(response3);
        Assert.assertTrue(response3 instanceof ListResponse);
        ListResponse listResponse2 = (ListResponse)response3;
        assertNotNull(listResponse2.getTopics());
        assertTrue(listResponse2.getTopics().length >= 3);
    }

}
