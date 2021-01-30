package com.viasat.burroughs;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class BurroughsServer {

    public static void main(String[] args) {
        try {
            int port = Integer.parseInt(args[0]);
            ServerSocket server = new ServerSocket(port);
            Socket clientSocket = server.accept();

        }
        catch(IOException e) {
            System.out.println("Failed to connect");
        }
        catch(ArrayIndexOutOfBoundsException | NumberFormatException e) {
            System.out.println("Please enter a valid port number");
        }
    }

}
