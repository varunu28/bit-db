package com.varun.db.command;

public record DeleteCommand(String key) implements Command {
    @Override
    public void execute() {

    }
}
