package main.java;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;

import java.util.ArrayList;
import java.util.Properties;

public class SentimentAnalyzer {

    public static ArrayList<String> getSentiments(String line) {

        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
        ArrayList<String> sentiments = new ArrayList<String>();
        String[] sentimentText = {"Very Negative", "Negative", "Neutral", "Positive", "Very Positive"};
        Annotation annotation = pipeline.process(line);
        for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
            Tree tree = sentence.get(SentimentCoreAnnotations.AnnotatedTree.class);
            int score = RNNCoreAnnotations.getPredictedClass(tree);
            sentiments.add(sentimentText[score]);
        }
        return sentiments;
    }
}
