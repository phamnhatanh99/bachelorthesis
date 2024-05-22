package rptu.thesis.npham.ds.utils;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringUtils {

    public static final String SEPARATOR = "__-__";

    private final static List<String> STOP_WORDS_LIST = Arrays.asList(
            "i", "me", "my", "myself",
            "we", "us", "our", "ours", "ourselves",
            "you", "your", "yours", "yourself", "yourselves",
            "he", "him", "his", "himself",
            "she", "her", "hers", "herself",
            "it", "its", "itself",
            "they", "them", "their", "theirs", "themselves",
            "what", "which", "who", "whom",
            "this", "that", "these", "those",
            "am", "is", "are", "was", "were", "be", "been", "being",
            "have", "has", "had", "having",
            "do", "does", "did", "doing",
            "would", "should", "could", "ought",
            "i'm", "you're", "he's", "she's", "it's", "we're", "they're", "i've", "you've", "we've", "they've",
            "i'd", "you'd", "he'd", "she'd", "we'd", "they'd",
            "i'll", "you'll", "he'll", "she'll", "we'll", "they'll",
            "isn't", "aren't", "wasn't", "weren't", "hasn't", "haven't", "hadn't", "doesn't", "don't", "didn't",
            "won't", "wouldn't", "shan't", "shouldn't", "can't", "cannot", "couldn't", "mustn't",
            "let's", "that's", "who's", "what's", "here's", "there's", "when's", "where's", "why's", "how's",
            "a", "an", "the",
            "and", "but", "if", "or", "because", "as", "until", "while",
            "of", "at", "by", "for", "with", "about", "against", "between",
            "into", "through", "during", "before", "after", "above", "below", "to", "from",
            "up", "down", "in", "out", "on", "off", "over", "under",
            "again", "further", "then", "once",
            "here", "there", "when", "where", "why", "how",
            "all", "any", "both", "each", "few", "more", "most",
            "other", "some", "such",
            "no", "nor", "not",
            "only", "own", "same",
            "so", "than", "too", "very");

    private static final List<String> STOP_WORDS = new ArrayList<>(STOP_WORDS_LIST);

    private static final Pattern ALPHANUMERIC = Pattern.compile("^(?:[0-9]+[a-zA-Z]|[a-zA-Z]+[0-9])[a-zA-Z0-9]*");;
    private static final Pattern CAPITALIZED = Pattern.compile("^[A-Z][a-z]+");
    private static final Pattern UPPERCASE = Pattern.compile("^[A-Z]+");
    private static final Pattern LOWERCASE = Pattern.compile("^[a-z]+");
    private static final Pattern NUMBER = Pattern.compile("^[0-9]+");
    private static final Pattern PUNCTUATION = Pattern.compile("^\\p{Punct}+");
    private static final Pattern WHITESPACE = Pattern.compile("^\\s+");
    private static final Pattern OTHER = Pattern.compile(".");

    public static String normalize(String s) {
        return s.trim().toLowerCase().replace(" ", "_");
    }

    public static List<String> removeStopWords (List<String> ls) {
        List<String> res = new ArrayList<>(ls);
        res.removeAll(STOP_WORDS);
        return res;
    }

    public static List<String> tokenize(String s) {
        List<String> res = Arrays.asList(s.split("\\s+|_+"));
        return new ArrayList<>(res);
    }

    public static Set<String> shingle(String s, int k) {
        if (k < 1) k = 4;
        Set<String> res = new HashSet<>();
        for (int i = 0; i < s.length() - k + 1; i++) {
            res.add(s.substring(i, i + k));
        }
        return res;
    }

    public static Set<String> generateFormatPatterns(Iterable<String> column) {
        Set<String> result = new HashSet<>();
        for (String value : column) {
            if (value != null && !value.trim().isEmpty()) {
                String tokenizedValue = fdTokenize(value.replaceAll("\n", " ").trim());
                result.add(getRegExString(tokenizedValue));
            }
        }
        return result;
    }

    /**
     * Adapted from <a href="https://doi.org/10.1109/ICDE48307.2020.00067">Dataset discovery in data lakes</a>
     */
    private static String fdTokenize(String str) {
        StringBuilder result = new StringBuilder();
        while (!str.isEmpty()) {
            Matcher a = ALPHANUMERIC.matcher(str);
            Matcher c = CAPITALIZED.matcher(str);
            Matcher u = UPPERCASE.matcher(str);
            Matcher l = LOWERCASE.matcher(str);
            Matcher n = NUMBER.matcher(str);
            Matcher p = PUNCTUATION.matcher(str);
            Matcher w = WHITESPACE.matcher(str);
            Matcher o = OTHER.matcher(str);

            if (a.find()) {
                result.append("a");
                str = str.substring(a.group().length());
            } else if (c.find()) {
                result.append("c");
                str = str.substring(c.group().length());
            } else if (u.find()) {
                result.append("u");
                str = str.substring(u.group().length());
            } else if (l.find()) {
                result.append("l");
                str = str.substring(l.group().length());
            } else if (n.find()) {
                result.append("n");
                str = str.substring(n.group().length());
            } else if (p.find()) {
                result.append("p");
                str = str.substring(p.group().length());
            } else if (w.find()) {
                result.append("w");
                str = str.substring(w.group().length());
            } else if (o.find()) {
                result.append("o");
                str = str.substring(o.group().length());
            } else {
                break;
            }
        }
        return result.toString();
    }

    private static String getRegExString(String str) {
        if (str == null || str.isEmpty()) return str;

        StringBuilder result = new StringBuilder();

        int length = str.length();

        for (int i = 0; i < length; i++) {
            result.append(str.charAt(i));
            int count = 1;
            while (i + 1 < length && str.charAt(i) == str.charAt(i + 1)) {
                count++;
                i++;
            }
            if (count > 1) {
                result.append('+');
            }
        }

        return result.toString();
    }
}
