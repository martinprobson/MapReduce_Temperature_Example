package net.martinprobson.hadoop.mr;

import org.apache.hadoop.io.Text;

/*
 * Simple helper class to parse a line for input to Mapper.
 */
class MaxTemperatureParser {
	public static final int MISSING = 9999;
	
	private Text year;
	private int temperature;
	private boolean valid;
	
	private MaxTemperatureParser() {}
	private MaxTemperatureParser(Boolean valid, Text year, int temperature) {
		this.valid = valid;
		this.year  = year;
		this.temperature = temperature;
	}
	
	boolean isValid() {
		return valid;
	}
	
	Text getYear() {
		return year;
	}
	
	int getTemperature() {
		return temperature;
	}
	
	public static MaxTemperatureParser parse(Text textLine) throws ParseException {
		
		String line = textLine.toString();
		String quality;
		Text year;
		int temperature;
		boolean valid;
		
		try {
			year = new Text(line.substring(15, 19));
		try {
		if (line.charAt(87) == '+') { // parseInt doesn't like leading plus signs
			temperature = Integer.parseInt(line.substring(88, 92));
		} else {
			temperature = Integer.parseInt(line.substring(87, 92));
		}
		} catch (NumberFormatException e) {
			throw new ParseException("Invalid temperature",e);
		}
		quality = line.substring(92, 93);
		} catch (StringIndexOutOfBoundsException e) {
			throw new ParseException("Line too short",e);
		}
		if (temperature != MISSING && quality.matches("[01459]")) 
			valid = true;
		else
			valid = false;
		
		return new MaxTemperatureParser(valid,year,temperature);
		
	}
}
