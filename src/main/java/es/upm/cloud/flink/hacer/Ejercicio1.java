package es.upm.cloud.flink.hacer;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Ejercicio1 {
    public static void main (String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> ds = executionEnvironment.readTextFile("./files/sensorData.csv");


        ds.writeAsText("./files/exercise1.csv", FileSystem.WriteMode.OVERWRITE);
        ds.print();


        executionEnvironment.execute("SourceSink");
    }

}
