package edu.purdue.datamine.burroughsserver;

import com.viasat.burroughs.Burroughs;
import com.viasat.burroughs.execution.ExecutionException;
import com.viasat.burroughs.service.model.burroughs.BurroughsConnection;
import com.viasat.burroughs.service.model.burroughs.QueryStatus;
import com.viasat.burroughs.service.model.description.Field;
import com.viasat.burroughs.service.model.list.Topic;
import com.viasat.burroughs.validation.TopicNotFoundException;
import com.viasat.burroughs.validation.UnsupportedQueryException;
import org.apache.calcite.sql.parser.SqlParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestController
public class CommandController {

    private final Burroughs burroughs;

    @Autowired
    public CommandController(Burroughs burroughs) {
        this.burroughs = burroughs;
    }

    @CrossOrigin(origins = "http://localhost:5000")
    @GetMapping("/command/connection")
    public BurroughsConnection getConnection() {
        return burroughs.connection();
    }

    @CrossOrigin(origins = "http://localhost:5000")
    @GetMapping("/command/topics")
    public Topic[] getTopics() {
        return burroughs.topics();
    }

    @CrossOrigin(origins = "http://localhost:5000")
    @GetMapping("/command/topic")
    public Field[] getSchema(@RequestParam(value = "topicName") String topicName) {
        return burroughs.topic(topicName);
    }

    @CrossOrigin(origins = "http://localhost:5000")
    @GetMapping("/command/table")
    public String getTable(){
        return burroughs.getDbTable();
    }

    @CrossOrigin(origins = "http://localhost:5000")
    @PostMapping("/command/table")
    public void setTable(@RequestParam(value = "tableName", defaultValue = "") String table) {
        if (table.length() < 1) throw new CommandException("Table name is required.");
        burroughs.setDbTable(table);
    }

    @CrossOrigin(origins = "http://localhost:5000")
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    @ExceptionHandler({ CommandException.class })
    public String handleException(CommandException e){
        return e.getMessage();
    }

    @CrossOrigin(origins = "http://localhost:5000")
    @PostMapping("/execute")
    public String execute(@RequestBody QueryBody query) {
        try {
            burroughs.processQuery(query.getQuery());
            return "Query Executed";
        } catch (SqlParseException | TopicNotFoundException | UnsupportedQueryException e) {
            return "Validation Error: " + e.getMessage();
        }
        catch (ExecutionException e) {
            return e.getMessage();
        }
    }

    @CrossOrigin(origins = "http://localhost:5000")
    @GetMapping("/command/status")
    public QueryStatus getStatus() {
        return burroughs.queryStatus();
    }

    @CrossOrigin(origins = "http://localhost:5000")
    @PostMapping("/command/stop")
    public void stop(@RequestParam(value = "keepTable", defaultValue = "false") boolean keepTable) {
        burroughs.stop(keepTable);
    }

}
