package edu.upenn.cis455.mapreduce.master;

public class Utilities {
	
	/**
	 * Create HTML page.
	 *
	 * @param title the title
	 * @param body the body
	 * @return byte array of HTML page
	 */
	public static String createHTML(String title, String body){
		String start =
				"<html>" +
				"<title>"+title+"</title>" +
				"<meta charset=\"utf-8\">"+
				"<body>";

		String end =
				"</body>" +
				"</html>";
		
		StringBuilder page = new StringBuilder();
		page.append(start);
		page.append(body);
		page.append(end);
		
		return page.toString();
		
		
	}


}
