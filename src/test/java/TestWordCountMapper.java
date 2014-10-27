/**
 * Created by daviss on 10/27/14.
 */

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;

import org.junit.Before;
import org.junit.Test;

public class TestWordCountMapper {

    MapDriver<Object, Text, Text, IntWritable> mapDriver;

    @Before
    public void setUp() {
        WordCountMapper mapper = new WordCountMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testMapper() throws java.io.IOException {

        mapDriver.withInput(new LongWritable(), new Text("a b c d e f g"));
        mapDriver.withOutput(new Text("a"), new IntWritable(1));
        mapDriver.withOutput(new Text("b"), new IntWritable(1));
        mapDriver.withOutput(new Text("c"), new IntWritable(1));
        mapDriver.withOutput(new Text("d"), new IntWritable(1));
        mapDriver.withOutput(new Text("e"), new IntWritable(1));
        mapDriver.withOutput(new Text("f"), new IntWritable(1));
        mapDriver.withOutput(new Text("g"), new IntWritable(1));
        mapDriver.runTest();

    }

}