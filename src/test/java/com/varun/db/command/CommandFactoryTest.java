package com.varun.db.command;

import com.varun.db.exception.InvalidCommandException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class CommandFactoryTest {

    @Test
    public void getCommandParsed_success() throws InvalidCommandException {
        String input = "    GET A     ";
        Command command = CommandFactory.parseCommand(input);
        assertTrue(command instanceof GetCommand);

        GetCommand getCommand = (GetCommand) command;
        assertEquals("A", getCommand.key());
    }

    @Test
    public void setCommandParsed_success() throws InvalidCommandException {
        String input = "    SET A      2      ";
        Command command = CommandFactory.parseCommand(input);
        assertTrue(command instanceof SetCommand);

        SetCommand setCommand = (SetCommand) command;
        assertEquals("A", setCommand.key());
        assertEquals("2", setCommand.value());
    }

    @Test
    public void delCommandParsed_success() throws InvalidCommandException {
        String input = "    DEL A     ";
        Command command = CommandFactory.parseCommand(input);
        assertTrue(command instanceof DeleteCommand);

        DeleteCommand deleteCommand = (DeleteCommand) command;
        assertEquals("A", deleteCommand.key());
    }
}
