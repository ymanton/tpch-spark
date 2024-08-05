package main.java;

import org.apache.spark.api.plugin.SparkPlugin;
import org.apache.spark.api.plugin.DriverPlugin;
import org.apache.spark.api.plugin.ExecutorPlugin;
import org.apache.spark.api.plugin.PluginContext;
import java.util.Map;

public class ExternalCommandPlugin implements SparkPlugin {
    @Override
    public DriverPlugin driverPlugin() {
        return null;
    }

    @Override
    public ExecutorPlugin executorPlugin() {
        return new ExecutorPlugin() {
            public void init(PluginContext ctx, Map<String,String> extraConf) {
                System.err.println("Executor plugin, executorID=" + ctx.executorID());
            }
        };
    }
}
