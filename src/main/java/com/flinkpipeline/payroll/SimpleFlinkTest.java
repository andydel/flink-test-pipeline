package com.flinkpipeline.payroll;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Simple Flink test job to verify infrastructure is working correctly. */
public class SimpleFlinkTest {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleFlinkTest.class);

  public static void main(String[] args) throws Exception {
    // Create Flink execution environment
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // Disable checkpointing for simple test
    env.getCheckpointConfig().disableCheckpointing();

    LOG.info("Starting Simple Flink Test Job");

    // Create a simple data stream
    DataStream<String> dataStream =
        env.addSource(
            new SourceFunction<String>() {
              private volatile boolean isRunning = true;
              private int counter = 0;

              @Override
              public void run(SourceContext<String> ctx) throws Exception {
                while (isRunning && counter < 100) {
                  String message = "Test message " + counter;
                  ctx.collect(message);
                  counter++;
                  Thread.sleep(1000); // Emit one message per second
                }
              }

              @Override
              public void cancel() {
                isRunning = false;
              }
            });

    // Print the stream
    dataStream.print();

    // Execute the job
    env.execute("Simple Flink Test Job");

    LOG.info("Simple Flink Test Job completed successfully");
  }
}
