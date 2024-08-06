package main.java;

import org.apache.spark.api.plugin.SparkPlugin;
import org.apache.spark.api.plugin.DriverPlugin;
import org.apache.spark.api.plugin.ExecutorPlugin;
import org.apache.spark.api.plugin.PluginContext;
import java.util.Map;

import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;

public class ExternalCommandPlugin implements SparkPlugin {
    @Override
    public DriverPlugin driverPlugin() {
        return null;
    }

    @Override
    public ExecutorPlugin executorPlugin() {
        return new ExecutorPlugin() {
            private ExternalCommandServer server;
            public void init(PluginContext ctx, Map<String,String> extraConf) {
                try {
                    server = new ExternalCommandServer();
                    ExternalCommand stub = (ExternalCommand)UnicastRemoteObject.exportObject(server, 0);
                    Registry registry = LocateRegistry.createRegistry(1099 + Integer.parseInt(ctx.executorID()));
                    registry.bind("ExternalCommand", stub);
                } catch (Exception e) {
                    System.err.println("Can't create ExternalCommand RMI server: " + e.toString());
                    e.printStackTrace();
                }
            }
        };
    }
}
