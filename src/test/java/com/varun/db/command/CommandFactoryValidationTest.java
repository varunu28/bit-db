package com.varun.db.command;

import com.varun.db.exception.InvalidCommandException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;

import static org.hamcrest.CoreMatchers.startsWith;

@RunWith(Parameterized.class)
public class CommandFactoryValidationTest {

    private final String invalidInput;
    private final String errorMessage;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    public CommandFactoryValidationTest(String invalidInput, String errorMessage) {
        this.invalidInput = invalidInput;
        this.errorMessage = errorMessage;
    }

    @Test
    public void invalidInputValidation_success() throws InvalidCommandException {
        thrown.expect(InvalidCommandException.class);
        thrown.expectMessage(startsWith(errorMessage));

        CommandFactory.parseCommand(invalidInput);
    }

    @Parameters
    public static Collection commandFactoryInputs() {
        return Arrays.asList(new String[][]{
                {"", "Operation needs to be specified in the command"},
                {"", "Operation needs to be specified in the command"},
                {"SET  ", "Operands needs to be specified in the command"},
                {"GET    ", "Operands needs to be specified in the command"},
                {"    DEL    ", "Operands needs to be specified in the command"},
                {"    SET A    ", "SET operation should contain value parameter"},
                {"    SET A  2 5  ", "SET operation should not contain parameters in addition to key & value"},
                {"    GET A 4   ", "GET/DEL operation should not contain parameters in addition to key"},
                {"    DEL A 6   ", "GET/DEL operation should not contain parameters in addition to key"},
                {"    UPDATE    ", "Operation not supported"},
        });
    }
}