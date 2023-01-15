package com.varun.db.command;

public record SetCommand(String key, String value) implements Command {
    @Override
    public void execute() {

    }
}
