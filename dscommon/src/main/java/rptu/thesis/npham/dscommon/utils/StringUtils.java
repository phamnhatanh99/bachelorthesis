package rptu.thesis.npham.dscommon.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

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

    /**
     * Normalize a string by trimming, lowercasing and replacing spaces with underscores
     * @param s input string
     * @return normalized string
     */
    public static String normalize(String s) {
        return s.trim().toLowerCase().replace(" ", "_");
    }

    /**
     * Tokenize a string by splitting on whitespace and underscores
     * @param s input string
     * @return list of tokens
     */
    public static List<String> tokenize(String s) {
        List<String> res = Arrays.asList(s.split("\\s+|_+"));
        return new ArrayList<>(res);
    }

    /**
     * Remove stop words from a list of strings, case-insensitive
     * @param ls input list of strings
     * @return list of strings with stop words removed
     */
    public static List<String> removeStopWords (List<String> ls) {
        List<String> res = ls.stream().map(String::toLowerCase).collect(Collectors.toCollection(ArrayList::new));
        res.removeAll(STOP_WORDS);
        return res;
    }
}
