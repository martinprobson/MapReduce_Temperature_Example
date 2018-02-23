package net.martinprobson.hadoop.mr;

import static org.junit.Assert.*;

import org.junit.Test;

import org.apache.hadoop.io.Text;
public class TestMaxTemperatureParser {

	private static final Text validLine = new Text("0081028640999991960012606004"
			+ "+65783+024583FM-12+001599999V0203501N00821002401CN0020"
			+ "001N9-00391-00501099871ADDAA199005091AY171999GF1089910"
			+ "81061002501999999KA1999N-00501MD1510001+9999MW1731");
	private static final Text invalidLine = new Text("0062010300999991959041108004"
			+ "+69700+019017FM-12+001499999V0200701N00461220001CN0600"
			+ "001N9+99999+99999101101ADDAY101999GF101991999001999999"
			+ "041001MW1021EQDQ01+000002SCOTLC");
	private static final Text invalidTemp = new Text("0062010300999991959041108004"
			+ "+69700+019017FM-12+001499999V0200701N00461220001CN0600"
			+ "001N9+99x99+99999101101ADDAY101999GF101991999001999999"
			+ "041001MW1021EQDQ01+000002SCOTLC");
	private static final Text shortLine = new Text("006");
	
	private static MaxTemperatureParser parser;
	
	@Test
	public void testIsValid() throws ParseException {
		parser = MaxTemperatureParser.parse(validLine);
		assertTrue("testIsValid: validLine",parser.isValid());
		parser = MaxTemperatureParser.parse(invalidLine);
		assertTrue("testIsValid: invalidLine",!parser.isValid());
	}

	@Test
	public void testGetYear() throws ParseException {
		parser = MaxTemperatureParser.parse(validLine);
		assertTrue("testGetYear: validLine",parser.getYear().equals(new Text("1960")));
		parser = MaxTemperatureParser.parse(invalidLine);
		assertTrue("testGetYear: invalidLine",parser.getYear().equals(new Text("1959")));
	}

	@Test
	public void testGetTemperature() throws ParseException {
		parser = MaxTemperatureParser.parse(validLine);
		assertTrue("testgetTemperature: validLine",parser.getTemperature() == -39 );
		parser = MaxTemperatureParser.parse(invalidLine);
		assertTrue("testgetTemperature: invalidLine",parser.getTemperature() == MaxTemperatureParser.MISSING);
	}
	
	@Test(expected = ParseException.class)
	public void testInvalidTemp() throws ParseException {
		parser = MaxTemperatureParser.parse(invalidTemp);
	}
	
	@Test(expected = ParseException.class)
	public void testShortLine() throws ParseException {
		parser = MaxTemperatureParser.parse(shortLine);
	}
	


}
