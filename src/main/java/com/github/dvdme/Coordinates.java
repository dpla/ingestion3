package com.github.dvdme;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Geographical Coordinates Object
 * 
 * Parses geographical coordinates in one of four formats:
 *  - degrees minutes seconds: 40� 26' 46'' N, 79� 58' 56'' W
 *  - degrees decimal minutes: 40� 26.767' N, 79� 58.933' W
 *  - decimal degrees: 40.446� N, 79.982� W
 *  - decimal: 40.446, 79.982
 * 
 * @author David
 * @version 1.0
 */

public class Coordinates {

	private double latitude;
	private double longitude;
	
	/**
	 * Constructor
	 * Initializes with 0 latitude and longitude  
	 */
	public Coordinates(){
		setLatitude(0);
		setLongitude(0);
	}

	/**
	 * Initializes with given latitude and longitude
	 * @param latitude Latitude
	 * @param longitude Longitude
	 */
	public Coordinates(String latitude, String longitude){
		setLatitude(parseCoordinate(latitude));
		setLongitude(parseCoordinate(longitude));
	}

	/**
	 * Initializes with given latitude and longitude
	 * @param latitude Latitude
	 * @param longitude Longitude
	 */
	public Coordinates(double latitude, double longitude){
		setLatitude(latitude);
		setLongitude(longitude);
	}

	/**
	 * Gets latitude
	 * @return latitude
	 */
	public double getLatitude() {
		return latitude;
	}
	
	/**
	 * Gets latitude as a string
	 * @return latitude as string
	 */
	public String getLatitudeAsString() {
		return String.valueOf(latitude);
	}

	/**
	 * Sets latitude
	 * @param latitude Latitude
	 */
	public void setLatitude(double latitude) {
		this.latitude = latitude;
	}

	/**
	 * Parses latitude
	 * @param latitude Latitude
	 */
	public void setLatitude(String latitude) {
		setLatitude( parseCoordinate(latitude) );
	}

	/**
	 * Gets longitude
	 * @return longitude Longitude
	 */
	public double getLongitude() {
		return longitude;
	}
	
	/**
	 * Gets longitude as string
	 * @return longitude as string
	 */
	public String getLongitudeAsString() {
		return String.valueOf(longitude);
	}

	/**
	 * Sets longitude
	 * @param longitude Longitude
	 */
	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}

	/**
	 * Parses longitude
	 * @param longitude Longitude
	 */
	public void setLongitude(String longitude) {
		setLongitude( parseCoordinate(longitude) );
	}
	
	@Override
	public String toString(){
		return String.valueOf(latitude) + "," + String.valueOf(longitude);
	} 

	private double parseCoordinate(String c){

		double retval = 0.0f;

		ArrayList<String> ar = parseIntsAndFloats(c);
		String [] c2 = new String[ar.size()];
		for(int i=0; i<ar.size(); i++){
			c2[i] = ar.get(i).replace(",", ".");
		}

		if(c2.length == 3){
			
			double degrees = Double.parseDouble(c2[0]);
			double minutes = Double.parseDouble(c2[1]);
			double seconds = Double.parseDouble(c2[2]);
			retval = degrees + minutes / 60 + seconds / 3600;
			
		} else if(c2.length == 2) {
			
			double degrees = Double.parseDouble(c2[0]);
			double minutes = Double.parseDouble(c2[1]);
			retval = degrees + minutes / 60;
			
		} else if(c2.length == 1) {
			
			retval = Double.parseDouble(c2[0]);
			
		} else {
			
			throw new IllegalArgumentException();
			
		}

		return isNegative(c) ? retval * -1 : retval;
	}

	private ArrayList<String> parseIntsAndFloats(String raw) {

		ArrayList<String> listBuffer = new ArrayList<String>();

		Pattern p = Pattern.compile("[-]?[0-9]*\\.?,?[0-9]+");

		Matcher m = p.matcher(raw);

		while (m.find()) {
			listBuffer.add(m.group());
		}

		return listBuffer;
	}

	private boolean isNegative(String raw) {

		boolean retval = false;
		raw = raw.toUpperCase();

		if( raw.contains("N") || raw.contains("E") )
			retval = false;
		else if(raw.contains("S") || raw.contains("W") )
			retval = true;

		return retval;
	}
	
}


