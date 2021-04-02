package com.viasat.burroughs;

import com.viasat.burroughs.service.SchemaService;
import com.viasat.burroughs.service.model.schema.Subject;
import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Test;

public class SchemaTest {
    @Test
    public void testLookupSchema() {
        SchemaService service = new SchemaService("http://localhost:8081");
        Subject s = service.getSchema("stock_trades");
        Assert.assertNotNull(s);
        Schema schema = s.getSchema();
        for (Schema.Field f : schema.getFields()) {
            System.out.println(f.name());
        }
    }
}
