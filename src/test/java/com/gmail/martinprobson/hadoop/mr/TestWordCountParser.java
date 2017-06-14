package com.gmail.martinprobson.hadoop.mr;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class TestWordCountParser {
	
	private TestCase testCase;

	static class TestCase {
		
		public final Text testCase;
		public final List<Text> expectedResult;
		
		TestCase(Text testCase,String expectedResult) {
			this.testCase = testCase;
			this.expectedResult = buildExpected(expectedResult);
		}
		
		private List<Text> buildExpected(String expected) {
			List<Text> expResult = new ArrayList<>();
			for(String w: expected.split("\n")) expResult.add(new Text(w));
			return expResult;
		}
	}
	
	@Parameters
	public static Collection<TestCase> testCases() {
		
		Collection<TestCase> cases = new ArrayList<>();
		cases.add(new TestCase(new Text("The Tragedie of Macbeth"),						"The\nTragedie\nof\nMacbeth"));
		cases.add(new TestCase(new Text("Who comes here?"),								"Who\ncomes\nhere"));
		cases.add(new TestCase(new Text("  3. A Drumme, a Drumme:"),					"3\nA\nDrumme\na\nDrumme"));
		cases.add(new TestCase(new Text("***Some>, Text&&&££"),        					"Some\nText"));
		cases.add(new TestCase(new Text("Accounted dangerous folly. Why then (alas)"), "Accounted\ndangerous\nfolly\nWhy\nthen\nalas"));
		
		return cases;
	}
	
	public TestWordCountParser(TestCase testCase) {
		this.testCase = testCase;
	}
	
	@Test
	public void testParse() {
		List<Text> actual 	= WordCountParser.parse(testCase.testCase);
		List<Text> expected	= testCase.expectedResult;
		assertTrue(actual.equals(expected));
	}

}
