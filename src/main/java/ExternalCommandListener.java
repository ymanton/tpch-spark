package main.java;

import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.SparkConf;
import java.lang.ProcessBuilder;
import java.io.IOException;

public class ExternalCommandListener extends SparkListener {
    private ProcessBuilder pb;

    public ExternalCommandListener(SparkConf conf) {
        pb = new ProcessBuilder(conf.get("spark-tpch.listener.command", "/usr/bin/false"), null);
    }

    @Override
    public void onJobStart(SparkListenerJobStart jobStart) {
        pb.command().set(1, "onJobStart");
        try {
            pb.start();
        }
        catch (IOException e) {
            System.err.println("External command on job start failed:\n" + e.getMessage());
        }
    }

    @Override
    public void onJobEnd(SparkListenerJobEnd jobEnd) {
        pb.command().set(1, "onJobEnd");
        try {
            pb.start();
        }
        catch (IOException e) {
            System.err.println("External command on job end failed:\n" + e.getMessage());
        }
    }
}
