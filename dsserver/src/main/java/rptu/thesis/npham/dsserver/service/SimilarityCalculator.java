package rptu.thesis.npham.dsserver.service;

import edu.stanford.nlp.simple.Sentence;
import edu.uniba.di.lacam.kdde.lexical_db.MITWordNet;
import edu.uniba.di.lacam.kdde.ws4j.RelatednessCalculator;
import edu.uniba.di.lacam.kdde.ws4j.similarity.Path;
import edu.uniba.di.lacam.kdde.ws4j.similarity.WuPalmer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import rptu.thesis.npham.dscommon.model.metadata.Metadata;
import rptu.thesis.npham.dscommon.utils.StringUtils;
import rptu.thesis.npham.dsserver.utils.Jaccard;
import tech.tablesaw.util.LevenshteinDistance;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Service
public class SimilarityCalculator {

    private final RelatednessCalculator rc;
    private final LSHIndex index;

    @Autowired
    public SimilarityCalculator(LSHIndex index) {
        rc = new WuPalmer(new MITWordNet());
        this.index = index;
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
        Set<String> lemma1 = new HashSet<>(rptu.thesis.npham.dscommon.utils.StringUtils.removeStopWords(l1));
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

    public Map<Metadata, Jaccard> tableNameShingleSimilarity(Metadata metadata) {
        return index.queryTableName(metadata);
    }

    public Map<Metadata, Jaccard> columnNameShingleSimilarity(Metadata metadata) {
        return index.queryColumnName(metadata);
    }

    public Map<Metadata, Jaccard> columnValuesSimilarity(Metadata metadata) {
        return index.queryColumnValue(metadata);
    }

    public Map<Metadata, Jaccard> columnFormatSimilarity(Metadata metadata) {
        return index.queryFormat(metadata);
    }
}
