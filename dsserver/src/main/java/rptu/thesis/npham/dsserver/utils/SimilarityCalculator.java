package rptu.thesis.npham.dsserver.utils;

import edu.stanford.nlp.simple.Sentence;
import edu.uniba.di.lacam.kdde.lexical_db.MITWordNet;
import edu.uniba.di.lacam.kdde.ws4j.RelatednessCalculator;
import edu.uniba.di.lacam.kdde.ws4j.similarity.WuPalmer;
import rptu.thesis.npham.dscommon.utils.StringUtils;
import tech.tablesaw.util.LevenshteinDistance;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Calculate semantic similarity between two strings using WordNet and Levenshtein distance
 */
public class SimilarityCalculator {

    private final RelatednessCalculator rc;

    public SimilarityCalculator() {
        rc = new WuPalmer(new MITWordNet());
    }

    public double levenshteinSimilarity(String s1, String s2) {
        Integer levenshtein = LevenshteinDistance.getDefaultInstance().apply(s1, s2);
        return 1 - ((double) levenshtein / Math.max(s1.length(), s2.length()));
    }

    /**
     * Calculate similarity between two sentences using WuPalmer WordNet similarity.
     * Score is calculated as the average of the maximum similarity between each word in the two sentences.
     */
    public double sentenceSimilarity(String sentence1, String sentence2) {
        Sentence s1 = new Sentence(StringUtils.tokenize(sentence1));
        Sentence s2 = new Sentence(StringUtils.tokenize(sentence2));
        List<String> l1 = s1.lemmas();
        List<String> l2 = s2.lemmas();
        Set<String> lemma1 = new HashSet<>(StringUtils.removeStopWords(l1));
        Set<String> lemma2 = new HashSet<>(StringUtils.removeStopWords(l2));

        double result = 0;

        for (String str1: lemma1)
            result += wordSentenceMaxSimilarity(str1, lemma2);
        for (String str2: lemma2)
            result += wordSentenceMaxSimilarity(str2, lemma1);

        return result / (lemma1.size() + lemma2.size());
    }

    /**
     * Calculate the maximum similarity between a word and a set of words.
     * If the similarity between two words is not found in WordNet, Levenshtein distance is used.
     */
    private double wordSentenceMaxSimilarity(String lemma, Iterable<String> lemmas) {
        double result = 0;
        for (String l : lemmas) {
            double similarity;
            try {
                similarity = rc.calcRelatednessOfWords(lemma, l);
            } catch (Exception e) {
                similarity = 0;
            }
            similarity = similarity > 0 ? similarity : levenshteinSimilarity(lemma, l);
            if (similarity > result) result = similarity;
        }
        return result;
    }
}
