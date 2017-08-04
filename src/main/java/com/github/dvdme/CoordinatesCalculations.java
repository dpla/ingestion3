package com.github.dvdme;

import java.util.Random;

/**
 * A utility class with static method to perform usefull
 * calculations with geographical coordinates
 *
 * @author David
 * @version 1.0
 */

public class CoordinatesCalculations {

    private CoordinatesCalculations() {
        //Empty private constructor to prevent instantiation
    }

    /**
     * Calculates if target is inside a square area where
     * center is the center of the square and radius is half
     * the side of the square.
     *
     * @param coordinate Target coordinate
     * @param center     Center coordinate
     * @param radius     Radius meters
     * @return True or False
     */
    public static boolean isInSquareArea(Coordinates coordinate, Coordinates center, int radius) {

        boolean latres = false;
        boolean lonres = false;

        Coordinates p1 = getDerivedPosition(center, radius, 0);
        Coordinates p2 = getDerivedPosition(center, radius, 90);
        Coordinates p3 = getDerivedPosition(center, radius, 180);
        Coordinates p4 = getDerivedPosition(center, radius, 270);

        if (coordinate.getLatitude() >= p3.getLatitude()
                && coordinate.getLatitude() <= p1.getLatitude())
            latres = true;

        if (coordinate.getLongitude() <= p2.getLongitude()
                && coordinate.getLongitude() >= p4.getLongitude())
            lonres = true;

        return (latres && lonres);
    }

    /**
     * Calculates if target is inside a circle area where
     * center is the center of the circle
     *
     * @param coordinate Target coordinate
     * @param center     Center coordinate
     * @param radius     Radius meters
     * @return True or False
     */
    public static boolean isInCircleArea(Coordinates coordinate, Coordinates center, int radius) {

        boolean res = isInSquareArea(coordinate, center, radius);
        if (getDistanceBetweenTwoPoints(coordinate, center) <= radius)
            res = true;
        else
            res = false;

        return res;

    }

    /**
     * Calculates the end-point from a given source at a given range (meters)
     * and bearing (degrees). This methods uses simple geometry equations to
     * calculate the end-point.
     *
     * @param coordinate Point of origin
     * @param range      Range in meters
     * @param bearing    Bearing in degrees
     * @return End-point from the source given the desired range and bearing.
     * @see <a href="stackoverflow.com">http://stackoverflow.com/questions/3695224/sqlite-getting-nearest-locations-with-latitude-and-longitude</a>
     */
    public static Coordinates getDerivedPosition(Coordinates coordinate, double range, double bearing) {

        double EarthRadius = 6371000; // meters

        double latA = Math.toRadians(coordinate.getLatitude());
        double lonA = Math.toRadians(coordinate.getLongitude());
        double angularDistance = range / EarthRadius;
        double trueCourse = Math.toRadians(bearing);

        double lat = Math.asin(Math.sin(latA) * Math.cos(angularDistance) + Math.cos(latA) * Math.sin(angularDistance) * Math.cos(trueCourse));

        double dlon = Math.atan2(Math.sin(trueCourse) * Math.sin(angularDistance) * Math.cos(latA), Math.cos(angularDistance) - Math.sin(latA) * Math.sin(lat));

        double lon = ((lonA + dlon + Math.PI) % (Math.PI * 2)) - Math.PI;

        lat = Math.toDegrees(lat);
        lon = Math.toDegrees(lon);

        return new Coordinates(lat, lon);

    }

    /**
     * Calculates the distance in meters between two points.
     *
     * @param c1 Coordinate one
     * @param c2 Coordinate two
     * @return The distance in meters
     * @see <a href="stackoverflow.com">http://stackoverflow.com/questions/3695224/sqlite-getting-nearest-locations-with-latitude-and-longitude</a>
     */
    public static double getDistanceBetweenTwoPoints(Coordinates c1, Coordinates c2) {

        double R = 6371000; // m
        double dLat = Math.toRadians(c2.getLatitude() - c1.getLatitude());
        double dLon = Math.toRadians(c2.getLongitude() - c1.getLongitude());
        double lat1 = Math.toRadians(c1.getLatitude());
        double lat2 = Math.toRadians(c2.getLatitude());

        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) + Math.sin(dLon / 2) * Math.sin(dLon / 2) * Math.cos(lat1) * Math.cos(lat2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        double d = R * c;

        return d;
    }

    /**
     * Gets a random location given a center point and a radius.
     *
     * @param centerLatitude  Center Latitude
     * @param centerLongitude Center Longitude
     * @param radius          Radius
     * @return Coordinates object
     * @see <a href="gis.stackexchange.com">http://gis.stackexchange.com/questions/25877/how-to-generate-random-locations-nearby-my-location</a>
     */
    public static Coordinates getRandomLocation(double centerLatitude, double centerLongitude, int radius) {

        Coordinates coor = new Coordinates(centerLatitude, centerLongitude);

        return getRandomLocation(coor, radius);
    }

    /**
     * Gets a random location given a center point and a radius.
     *
     * @param center Center
     * @param radius Radius
     * @return Coordinates object
     * @see <a href="gis.stackexchange.com">http://gis.stackexchange.com/questions/25877/how-to-generate-random-locations-nearby-my-location</a>
     */
    public static Coordinates getRandomLocation(Coordinates center, int radius) {

        Random random = new Random();

        // Convert radius from meters to degrees
        double radiusInDegrees = radius / 111000f;

        double u = random.nextDouble();
        double v = random.nextDouble();
        double w = radiusInDegrees * Math.sqrt(u);
        double t = 2 * Math.PI * v;
        double x = w * Math.cos(t);
        double y = w * Math.sin(t);

        // Adjust the x-coordinate for the shrinking of the east-west distances
        double new_x = x / Math.cos(center.getLongitude());

        double foundLatitude = new_x + center.getLatitude();
        double foundLongitude = y + center.getLongitude();

        return new Coordinates(foundLatitude, foundLongitude);
    }

}
