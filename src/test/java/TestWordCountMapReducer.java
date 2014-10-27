/**
 * Created by daviss on 10/27/14.
 */

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;

import org.junit.Before;
import org.junit.Test;


public class TestWordCountMapReducer {

    MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    public void setUp() {
        WordCountMapper mapper = new WordCountMapper();
        WordCountReducer reducer = new WordCountReducer();
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapReducer() throws java.io.IOException {

        mapReduceDriver.withInput(new LongWritable(), new Text("a b c d e f g a a b b c"));
        mapReduceDriver.withOutput(new Text("a"), new IntWritable(3));
        mapReduceDriver.withOutput(new Text("b"), new IntWritable(3));
        mapReduceDriver.withOutput(new Text("c"), new IntWritable(2));
        mapReduceDriver.withOutput(new Text("d"), new IntWritable(1));
        mapReduceDriver.withOutput(new Text("e"), new IntWritable(1));
        mapReduceDriver.withOutput(new Text("f"), new IntWritable(1));
        mapReduceDriver.withOutput(new Text("g"), new IntWritable(1));

        mapReduceDriver.runTest();

    }
}
