package com.gmail.martinprobson.hadoop.mr;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.gmail.martinprobson.hadoop.util.HDFSUtil;




public class TestMapReduce {
	
	/**
	 * The output directory for results.
	 */
	public static final String TEST_OUTPUT_DIR = "output";
	
	/**
	 * The Configutation object used for the test.
	 */
	private Configuration conf;

	/**
	 * This class sets up a rule to ensure the output results are not deleted in the
	 * event of a test failure.
	 * @author martinr
	 *
	 */
	public class ConditionalTeardown implements TestRule {
		public Statement apply(Statement base, Description description) {
			return statement(base,description);
		}
		
		private Statement statement(Statement base, Description desc) {
			return new Statement() {
				@Override
				public void evaluate() throws Throwable {
					try {
						base.evaluate();
						tearDown();
					} catch (Throwable e) {
						// No teardown
						throw e;
					}
				}
			};
		}
		
	}
	
	/**
	 * Setup new configuration for test.
	 */
	@Before
	public void setUp() throws Exception {
		conf = new Configuration();
	}
	
	
	@Rule
	public ConditionalTeardown conditionalTeardown = new ConditionalTeardown();
	
	@Test
	public void testMapReduce() throws Exception {
		
		// The input data dir needs to be in the Maven test/resources directory.
		Path input  = new Path(TestMapReduce.class.getResource("/test_data").toString());
		Path output = new Path(TEST_OUTPUT_DIR);
				
		HDFSUtil.deletePath(conf,output);
		
		WordCountDriver driver = new WordCountDriver();
		driver.setConf(conf);
		
		int exitCode = driver.run(new String[] {
				input.toString(),
				output.toString()
		});
		assertTrue(exitCode == 0 );
		
		URI expectedResultsFile = TestMapReduce.class.getResource("/TestMapReduce_expected_results.txt").toURI();
		List<String> expectedResults = FileUtils.readLines(new File(expectedResultsFile),Charset.defaultCharset());
		Collections.sort(expectedResults, (s1, s2) -> s1.compareTo(s2));
		List<String> actualResults   = HDFSUtil.readFileasList(conf,new Path(TEST_OUTPUT_DIR + "/part-r-00000"));
		Collections.sort(actualResults, (s1, s2) -> s1.compareTo(s2));
		
		assertTrue(expectedResults.equals(actualResults));
		
	}
	
	/**
	 * Delete the output directory.
	 */
	public void tearDown() throws Exception {
		HDFSUtil.deletePath(conf,new Path(TEST_OUTPUT_DIR));
	}


}
