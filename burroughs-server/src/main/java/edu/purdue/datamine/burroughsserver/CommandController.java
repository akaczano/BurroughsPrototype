package edu.purdue.datamine.burroughsserver;

import com.viasat.burroughs.Burroughs;
import com.viasat.burroughs.execution.ExecutionException;
import com.viasat.burroughs.service.model.burroughs.BurroughsConnection;
import com.viasat.burroughs.service.model.burroughs.QueryStatus;
import com.viasat.burroughs.service.model.description.Field;
import com.viasat.burroughs.service.model.list.Topic;
import com.viasat.burroughs.validation.TopicNotFoundException;
import com.viasat.burroughs.validation.UnsupportedQueryException;
import edu.purdue.datamine.burroughsserver.model.ProducerModel;
import edu.purdue.datamine.burroughsserver.model.QueryBody;
import edu.purdue.datamine.burroughsserver.model.DatabaseProperties;
import org.apache.calcite.sql.parser.SqlParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@RestController
public class CommandController {

    private final ListLogger logger;
    private final Burroughs burroughs;
    private final ConnectionHolder conn;

    @Autowired
    public CommandController(ListLogger logger, Burroughs burroughs, ConnectionHolder conn) {
        this.logger = logger;
        this.burroughs = burroughs;
        this.conn = conn;
    }

    @CrossOrigin
    @GetMapping("/command/connection")
    public BurroughsConnection getConnection() {
        return burroughs.connection();
    }

    @CrossOrigin
    @GetMapping("/command/topics")
    public Topic[] getTopics() {
        return burroughs.topics();
    }

    @CrossOrigin
    @GetMapping("/command/topic")
    public Field[] getSchema(@RequestParam(value = "topicName") String topicName) {
        return burroughs.topic(topicName);
    }

    @CrossOrigin
    @GetMapping("/command/table")
    public String getTable(){
        return burroughs.getDbTable();
    }

    @CrossOrigin
    @PostMapping("/command/table")
    public void setTable(@RequestParam(value = "tableName", defaultValue = "") String table) {
        if (table.length() < 1) throw new CommandException("Table name is required.");
        burroughs.setDbTable(table);
    }

    @CrossOrigin
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    @ExceptionHandler({ CommandException.class })
    public String handleException(CommandException e){
        return e.getMessage();
    }

    @CrossOrigin
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    @ExceptionHandler({ExecutionException.class})
    public String handleException(ExecutionException e) {
        return e.getMessage();
    }

    @CrossOrigin
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    @ExceptionHandler({TopicNotFoundException.class, SqlParseException.class, UnsupportedQueryException.class})
    public String handleException(Exception e) {
        return "Validation Error: " + e.getMessage();
    }

    @CrossOrigin
    @PostMapping("/execute")
    public void execute(@RequestBody QueryBody query) throws
            SqlParseException, TopicNotFoundException,
            UnsupportedQueryException, ExecutionException {
            burroughs.processQuery(query.getQuery());
    }

    @CrossOrigin
    @GetMapping("/command/status")
    public QueryStatus getStatus() {
        return burroughs.queryStatus();
    }

    @CrossOrigin
    @PostMapping("/command/stop")
    public void stop(@RequestParam(value = "keepTable", defaultValue = "false") boolean keepTable) {
        burroughs.stop(keepTable);
    }

    @CrossOrigin
    @GetMapping("/database")
    public DatabaseProperties database() {
        if (!burroughs.connection().isDbConnected()) {
            return null;
        }
        DatabaseProperties props = new DatabaseProperties();
        props.setDatabase(burroughs.getDatabase());
        props.setTable(burroughs.getDbTable() == null ? "" : burroughs.getDbTable());
        props.setHostname(burroughs.getDbHost());
        props.setUsername(burroughs.getDbUser());
        return props;
    }

    @CrossOrigin
    @GetMapping("/console")
    public Object[] getMessages(@RequestParam long lastQuery) {
        return logger.getMessages(lastQuery);
    }

    @CrossOrigin
    @GetMapping("/producer")
    public List<ProducerModel> getProducers() {
        List<ProducerModel> models = new ArrayList<>();
        Set<String> producerNames = burroughs.producerInterface().getList();
        for (String name : producerNames) {
            ProducerModel producer = new ProducerModel(name,
                    burroughs.producerInterface().getProducerStatus(name));
            producer.setDelay(burroughs.producerInterface().getProducerDelay(name));
            models.add(producer);
        }
        return models;
    }


    @CrossOrigin
    @PostMapping("/producer/{name}/start")
    public void startProducer(@PathVariable(value = "name") String name,
                              @RequestParam(value = "limit", defaultValue = "-1") int limit){
        if (!burroughs.producerInterface().hasProducer(name)) {
            throw new CommandException("Producer not found");
        }
        burroughs.producerInterface().startProducer(name, limit);
    }

    @CrossOrigin
    @PostMapping("/producer/{name}/setdelay")
    public int setDelay(@PathVariable(value = "name") String name,
                        @RequestParam(value = "delay", defaultValue = "0") int delay) {
        if (!burroughs.producerInterface().hasProducer(name)) {
            throw new CommandException("Producer not found");
        }
        burroughs.producerInterface().setProducerDelay(name, delay);
        return delay;
    }

    @CrossOrigin
    @PostMapping("/producer/{name}/pause")
    public void pauseProducer(@PathVariable(value = "name") String name,
                        @RequestParam(value = "time", defaultValue = "-1") int time) {
        if (!burroughs.producerInterface().hasProducer(name)) {
            throw new CommandException("Producer not found");
        }
        if (time > 0) {
            burroughs.producerInterface().pauseProducer(name, time);
        }
        else {
            burroughs.producerInterface().pauseProducer(name);
        }
    }
    @CrossOrigin
    @PostMapping("/producer/{name}/kill")
    public void killProducer(@PathVariable(value = "name") String name) {
        if (!burroughs.producerInterface().hasProducer(name)) {
            throw new CommandException("Producer not found");
        }
        burroughs.producerInterface().terminateProducer(name);
    }

    @CrossOrigin
    @PostMapping("/producer/{name}/resume")
    public void resumeProducer(@PathVariable(value = "name") String name) {
        if (!burroughs.producerInterface().hasProducer(name)) {
            throw new CommandException("Producer not found");
        }
        burroughs.producerInterface().resumeProducer(name);
    }

    @CrossOrigin
    @PostMapping("/command/connect")
    public void connect() {
        burroughs.init();
        if (burroughs.connection().isDbConnected()) {
            conn.init();
        }
    }

    @CrossOrigin
    @GetMapping("/data")
    public List<Object[]> getData(@RequestParam(value = "query", defaultValue = "") String query) {
        if (query.length() < 1) {
            throw new CommandException("Please specify a query");
        }
        try {
            return conn.getSnapshot(query);
        } catch(SQLException e) {
            throw new CommandException(e.getMessage());
        }
    }
}
