package main.java;

import org.apache.spark.SparkConf;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerExecutorAdded;
import org.apache.spark.scheduler.SparkListenerExecutorRemoved;
import java.lang.ProcessBuilder;
import java.io.IOException;

import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;

import java.util.Map;
import java.util.HashMap;

public class ExternalCommandListener extends SparkListener {
    private ProcessBuilder pb;
    private Map<Integer, ExternalCommand> remotes = new HashMap();
    private String executorListenerCommand;

    public ExternalCommandListener(SparkConf conf) {
        pb = new ProcessBuilder(System.getenv().getOrDefault("TPCH_SPARK_DRIVER_LISTENER_COMMAND", "/usr/bin/false"), null);
        pb.inheritIO();
        executorListenerCommand = System.getenv().getOrDefault("TPCH_SPARK_EXECUTOR_LISTENER_COMMAND", "/usr/bin/false");
    }

    @Override
    public void onExecutorAdded(SparkListenerExecutorAdded executorAdded) {
        int executorID = Integer.parseInt(executorAdded.executorId());
        try {
            Registry registry = LocateRegistry.getRegistry(executorAdded.executorInfo().executorHost(), 1099 + executorID);
            ExternalCommand remote = (ExternalCommand)registry.lookup("ExternalCommand");
            remotes.put(new Integer(executorID), remote);
        } catch (Exception e) {
            System.err.println("Can't connect to ExternalCommand RMI server: " + e.toString());
            e.printStackTrace();
        }
    }

    @Override
    public void onExecutorRemoved(SparkListenerExecutorRemoved executorRemoved) {
        remotes.remove(new Integer(executorRemoved.executorId()));
    }

    @Override
    public void onJobStart(SparkListenerJobStart jobStart) {
        pb.command().set(1, "onJobStart");
        try {
            pb.start();
            for (Map.Entry<Integer, ExternalCommand> remote : remotes.entrySet()) {
                remote.getValue().run(executorListenerCommand, "onJobStart");
            }
        }
        catch (IOException e) {
            System.err.println("External command on job start failed: " + e.toString());
            e.printStackTrace();
        }
    }

    @Override
    public void onJobEnd(SparkListenerJobEnd jobEnd) {
        pb.command().set(1, "onJobEnd");
        try {
            pb.start();
            for (Map.Entry<Integer, ExternalCommand> remote : remotes.entrySet()) {
                remote.getValue().run(executorListenerCommand, "onJobEnd");
            }
        }
        catch (IOException e) {
            System.err.println("External command on job end failed: " + e.toString());
            e.printStackTrace();
        }
    }
}
