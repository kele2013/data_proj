package org.apache.nifi.processor.StreamLoad;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * Created by coco1 on 2017/7/20.
 */
public class NebulaProcessorTest {
    @org.junit.Test
    public void testOnTrigger() throws IOException {
        // Content to be mock a json file
        InputStream content = new ByteArrayInputStream("{\"hello\":\"nifi rocks\"}".getBytes());

        // Generate a test runner to mock a processor in a flow
        TestRunner runner = TestRunners.newTestRunner(new StreamLoadProcessor());

        // Add properites
        runner.setProperty(StreamLoadProcessor.JSON_PATH, "$.hello");

        // Add the content to the runner
        runner.enqueue(content);

        // Run the enqueued content, it also takes an int = number of contents queued
        runner.run(1);

        // All results were processed with out failure
        runner.assertQueueEmpty();

        // If you need to read or do aditional tests on results you can access the content
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(StreamLoadProcessor.SUCCESS);
        assertTrue("1 match", results.size() == 1);
        MockFlowFile result = results.get(0);
        String resultValue = new String(runner.getContentAsByteArray(result));
        System.out.println("Match: " + IOUtils.toString(runner.getContentAsByteArray(result)));

        // Test attributes and content
       // result.assertAttributeEquals(NebulaProcessor.MATCH_ATTR, "nifi rocks");
        result.assertContentEquals("nifi rocks");
    }
}
