package rptu.thesis.npham.ds.utils;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringUtils {

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

    private static final Pattern SYMBOL_PATTERN = Pattern.compile("@\\p{Sc}");
    private static final Pattern PUNCTUATION_PATTERN = Pattern.compile("[!\"#%&'()*+,\\-./:;<=>?\\[\\\\\\]^_`{|}~]");

    private static final Pattern LOWERCASE = Pattern.compile("[a-z]([a-z\\-])*");
    private static final Pattern UPPERCASE = Pattern.compile("[A-Z]([A-Z\\-.])*");
    private static final Pattern CAPITALIZED = Pattern.compile("[A-Z][a-z]([a-z\\-])*");

    private static final Pattern POS_DECIMAL = Pattern.compile("\\+?[0-9]+(,[0-9]+)*\\.[0-9]+");
    private static final Pattern NEG_DECIMAL = Pattern.compile("-[0-9]+(,[0-9]+)*\\.[0-9]+");
    private static final Pattern POS_INT = Pattern.compile("\\+?[0-9]+(,[0-9]+)*");
    private static final Pattern NEG_INT = Pattern.compile("-[0-9]+(,[0-9]+)*");

    private static final Pattern PUNCTUATION = Pattern.compile("[" + PUNCTUATION_PATTERN + "]+");
    private static final Pattern SYMBOLS = Pattern.compile("[" + SYMBOL_PATTERN + "]+", Pattern.UNICODE_CHARACTER_CLASS);
    private static final Pattern WHITESPACE = Pattern.compile("\\s+");

    static final Pattern ALPHA_NUM = Pattern.compile("(?:[0-9]+[a-zA-Z]|[a-zA-Z]+[0-9])[a-zA-Z0-9]*");
    static final Pattern NUM_SYMBOL = Pattern.compile("(?=.*[0-9,.])" + "(?=.*[" + SYMBOL_PATTERN + "]+)" + "([0-9" + SYMBOL_PATTERN + "]+)", Pattern.UNICODE_CHARACTER_CLASS);

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

    public static Set<String> generateFormatPatterns(Iterable<String> column) {
        Set<String> result = new HashSet<>();
        for (String value : column) {
            if (value != null && !value.trim().isEmpty()) {
                String formattedValue = value.replaceAll("\n", " ").trim();
                String tokenizedValue = fdTokenize(formattedValue);
                result.add(toRegEx(tokenizedValue));
            }
        }
        return result;
    }

    /**
     * Taken from <a href="https://doi.org/10.1109/ICDE48307.2020.00067">Dataset discovery in data lakes</a>
     */
    private static String fdTokenize(String str) {
        StringBuilder result = new StringBuilder();
        while (!str.isEmpty()) {
            Matcher c = CAPITALIZED.matcher(str);
            Matcher u = UPPERCASE.matcher(str);
            Matcher l = LOWERCASE.matcher(str);

            Matcher d = POS_DECIMAL.matcher(str);
            Matcher e = NEG_DECIMAL.matcher(str);
            Matcher i = POS_INT.matcher(str);
            Matcher j = NEG_INT.matcher(str);

            Matcher p = PUNCTUATION.matcher(str);
            Matcher s = SYMBOLS.matcher(str);
            Matcher w = WHITESPACE.matcher(str);

            Matcher a = ALPHA_NUM.matcher(str);
            Matcher q = NUM_SYMBOL.matcher(str);

            if (a.find()) {
                String tok = a.group();
                result.append("a");
                str = str.substring(tok.length());
            } else if (d.find()) {
                String tok = d.group();
                result.append("d");
                str = str.substring(tok.length());
            } else if (e.find()) {
                String tok = e.group();
                result.append("e");
                str = str.substring(tok.length());
            } else if (i.find()) {
                String tok = i.group();
                result.append("i");
                str = str.substring(tok.length());
            } else if (j.find()) {
                String tok = j.group();
                result.append("j");
                str = str.substring(tok.length());
            } else if (q.find()) {
                String tok = q.group();
                result.append("q");
                str = str.substring(tok.length());
            } else if (c.find()) {
                String tok = c.group();
                result.append("c");
                str = str.substring(tok.length());
            } else if (u.find()) {
                String tok = u.group();
                result.append("u");
                str = str.substring(tok.length());
            } else if (l.find()) {
                String tok = l.group();
                result.append("l");
                str = str.substring(tok.length());
            } else if (p.find()) {
                String tok = p.group();
                result.append("p");
                str = str.substring(tok.length());
            } else if (s.find()) {
                String tok = s.group();
                result.append("s");
                str = str.substring(tok.length());
            } else if (w.find()) {
                String tok = w.group();
                result.append("w");
                str = str.substring(tok.length());
            } else {
                break;
            }
        }
        return result.toString();
    }

    private static String toRegEx(String str) {
        StringBuilder result = new StringBuilder();

        char prevChar = '\0'; // initialize prevChar with a non-character

        for (char c : str.toCharArray()) {
            if (c != prevChar)
                result.append(c).append("+");
            prevChar = c;
        }

        return result.toString();
    }
}
