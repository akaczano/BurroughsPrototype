package com.viasat.burroughs;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

/**
 * Unit test for simple App.
 */
public class AppTest 
{
    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAnswerWithTrue() throws FileNotFoundException {
        Scanner scanner = new Scanner(new File("/home/aidan/IdeaProjects/BurroughsPrototype/producer/datafiles/transactions.csv"));
        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            for (char c : line.toCharArray()) {
                if ((int)c < 32) System.out.println("AHA!");
            }
        }

    }
}
