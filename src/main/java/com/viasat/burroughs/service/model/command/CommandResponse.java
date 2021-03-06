package com.viasat.burroughs.service.model.command;

import com.viasat.burroughs.service.model.StatementResponse;

/*
    Response object for CREATE, DROP, and TERMINATE commands
 */
public class CommandResponse extends StatementResponse {
    private String commandId;
    private CommandStatus commandStatus;
    private int commandSequenceNumber;

    public String getCommandId() {
        return commandId;
    }

    public void setCommandId(String commandId) {
        this.commandId = commandId;
    }

    public CommandStatus getCommandStatus() {
        return commandStatus;
    }

    public void setCommandStatus(CommandStatus commandStatus) {
        this.commandStatus = commandStatus;
    }

    public int getCommandSequenceNumber() {
        return commandSequenceNumber;
    }

    public void setCommandSequenceNumber(int commandSequenceNumber) {
        this.commandSequenceNumber = commandSequenceNumber;
    }

    public String toString() {
        return String.format("%s id has status %s\n",getCommandId(),getCommandStatus().getMessage() + " : " + getCommandStatus().getStatus());
    }
}
