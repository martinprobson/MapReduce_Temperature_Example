package net.martinprobson.hadoop.mr;
/**
 * Exception class for parse exceptions.
 * @author robsom12
 *
 */
public class ParseException extends Exception {

	private static final long serialVersionUID = 1L;

	/**
	 * @param msg
	 */
	public ParseException(final String msg) {
		super(msg);
	}
	
	public ParseException(final String msg, Throwable cause) {
		super(msg,cause);
	}

}
