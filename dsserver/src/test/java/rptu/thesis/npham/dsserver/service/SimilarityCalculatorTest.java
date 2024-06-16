package rptu.thesis.npham.dsserver.service;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SimilarityCalculatorTest {

    private final SimilarityCalculator similarityCalculator = new SimilarityCalculator();

    @Test
    public void testLevenshteinSimilarity() {
        String s1 = "hello";
        String s2 = "hello";
        double similarity = similarityCalculator.levenshteinSimilarity(s1, s2);
        Assertions.assertEquals(1.0, similarity);

        s1 = "hello";
        s2 = "world";
        similarity = similarityCalculator.levenshteinSimilarity(s1, s2);
        Assertions.assertTrue(Math.abs(similarity - 0.2) < 0.0001);
    }

    @Test
    public void testSentenceSimilarity() {
        String s1 = "hello world";
        String s2 = "hello world";
        double similarity = similarityCalculator.sentenceSimilarity(s1, s2);
        Assertions.assertEquals(1.0, similarity);

        s1 = "hello world";
        s2 = "world hello";
        similarity = similarityCalculator.sentenceSimilarity(s1, s2);
        Assertions.assertEquals(1.0, similarity);

        s1 = "hello world";
        s2 = "world";
        similarity = similarityCalculator.sentenceSimilarity(s1, s2);
        Assertions.assertTrue(similarity < 0.85 && similarity > 0.79);

        s1 = "he123";
        s2 = "he1";
        similarity = similarityCalculator.sentenceSimilarity(s1, s2);
        Assertions.assertTrue(Math.abs(similarity - 0.6) < 0.0001);
    }

}
