package rptu.thesis.npham.ds.utils;

public class StringUtils {
    public static String normalize(String s) {
        return s.trim().toLowerCase().replace(" ", "_");
    }
}
