package rptu.thesis.npham.ds.service;

import edu.stanford.nlp.simple.Sentence;
import edu.uniba.di.lacam.kdde.lexical_db.ILexicalDatabase;
import edu.uniba.di.lacam.kdde.lexical_db.MITWordNet;
import edu.uniba.di.lacam.kdde.ws4j.RelatednessCalculator;
import edu.uniba.di.lacam.kdde.ws4j.similarity.Lin;
import edu.uniba.di.lacam.kdde.ws4j.similarity.Path;
import edu.uniba.di.lacam.kdde.ws4j.similarity.WuPalmer;
import org.springframework.stereotype.Service;
import rptu.thesis.npham.ds.utils.StringUtils;
import tech.tablesaw.util.LevenshteinDistance;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Service
public class Similarity {

    public static final int LIN_SIMILARITY = 0;
    public static final int WUP_SIMILARITY = 1;
    public static final int PATH_SIMILARITY = 2;

    private final ILexicalDatabase db = new MITWordNet();
    private final RelatednessCalculator rc;

    public Similarity() {
        rc = new Path(db);
    }

    public Similarity(int similarity_algorithm) {
        switch (similarity_algorithm) {
            case LIN_SIMILARITY -> rc = new Lin(db);
            case WUP_SIMILARITY -> rc = new WuPalmer(db);
            case PATH_SIMILARITY -> rc = new Path(db);
            default -> throw new IllegalArgumentException("Invalid similarity algorithm");
        }
    }

    public double levenshteinSimilarity(String s1, String s2) {
        Integer levenshtein = LevenshteinDistance.getDefaultInstance().apply(s1, s2);
        return 1 - ((double) levenshtein / Math.max(s1.length(), s2.length()));
    }

    public double sentenceSimilarity(String sentence1, String sentence2) {
        Sentence s1 = new Sentence(sentence1);
        Sentence s2 = new Sentence(sentence2);
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

    private double wordSentenceMaxSimilarity(String lemma, Iterable<String> lemmas) {
        double result = 0;
        for (String l : lemmas) {
            double similarity = rc.calcRelatednessOfWords(lemma, l);
            similarity = similarity > 0 ? similarity : levenshteinSimilarity(lemma, l);
            if (similarity > result) result = similarity;
        }
        return result;
    }

}
