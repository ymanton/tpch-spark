package main.java;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface ExternalCommand extends Remote {
    boolean run(String... command) throws RemoteException;
}
