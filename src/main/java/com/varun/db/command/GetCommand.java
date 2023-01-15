package com.varun.db.command;

public record GetCommand(String key) implements Command {
    @Override
    public void execute() {

    }
}
