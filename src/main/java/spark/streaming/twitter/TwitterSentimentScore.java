package spark.streaming.twitter;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.*;
import org.apache.spark.*;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import scala.Tuple2;
import twitter4j.Status;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class TwitterSentimentScore {

    public static void main(String args[]) throws IOException, InterruptedException {
        computeSentimentScore();
    }

    private static void computeSentimentScore() throws IOException, InterruptedException {
        // configure Spark
        JavaSparkContext sc = getOrCreateSparkContext();
        JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(10));

        // Creating DStream from Twitter and filter out english only
        JavaDStream<Status> tweets = TwitterUtils.createStream(jssc).filter(status -> status.getLang() == "en");

        // Using different set of words used to filter and score each word of a sentence.
        // Because these lists are pretty small
        // it can be worthwhile to broadcast those across the cluster so that every executor can access them locally
        Broadcast<Set<String>> uselessWords = sc.broadcast(Utils.load("/stop-words.dat"));
        Broadcast<Set<String>> positiveWords = sc.broadcast(Utils.load("/pos-words.dat"));
        Broadcast<Set<String>> negativeWords = sc.broadcast(Utils.load("/neg-words.dat"));

        // extract the words of each tweet
        // We'll carry the tweet along in order to print it in the end
        JavaDStream<Tuple2<String, List<String>>> textAndSentences = tweets
                .map(tweet -> tweet.getText())
                .map(tweetText -> new Tuple2<>(tweetText, Utils.wordsOf(tweetText)));

        // Apply several transformations that allow us to keep just meaningful sentences
        JavaDStream<Tuple2<String, List<String>>> textAndMeaningfulSentences = getMeaningFulSentences(uselessWords, textAndSentences);

        // Compute the score of each sentence and keep only the non-neutral ones
        JavaDStream<Tuple2<String, Integer>> textAndNonNeutralScore = computeScoreStream(positiveWords, negativeWords, textAndMeaningfulSentences);

        // Transform the (tweet, score) pair into a readable string and print it
        textAndNonNeutralScore.map(Utils::makeReadable).print();
        textAndNonNeutralScore.dstream().saveAsTextFiles("tweets", "json");

        jssc.start();
        jssc.awaitTermination();
    }

    private static JavaDStream<Tuple2<String, Integer>> computeScoreStream(Broadcast<Set<String>> positiveWords, Broadcast<Set<String>> negativeWords, JavaDStream<Tuple2<String, List<String>>> textAndMeaningfulSentences) {
        return textAndMeaningfulSentences.map(x -> {
            int score = Utils.computeScore(x._2, positiveWords.getValue(), negativeWords.getValue());
            return new Tuple2<>(x._1, score);
        }).filter(x -> x._2 != 0);
    }

    private static JavaDStream<Tuple2<String, List<String>>> getMeaningFulSentences(Broadcast<Set<String>> uselessWords, JavaDStream<Tuple2<String, List<String>>> textAndSentences) {
        return textAndSentences.map(tuple -> {
            List<String> lowercaseList = Utils.toLowerCase(tuple._2);
            List<String> actualWords = Utils.keepActualWords(lowercaseList);
            List<String> meaningFulWords = Utils.keepMeaningfulWords(actualWords, uselessWords.getValue());
            return new Tuple2<>(tuple._1, meaningFulWords);
        }).filter(newTuple -> newTuple._2.size() > 0);
    }

    private static JavaSparkContext getOrCreateSparkContext() {
        SparkConf conf = new SparkConf()
                .setMaster(Utils.getSysParam("spark.master", "local[*]"))
                .setAppName(Utils.getSysParam("spark.appName", "sentiment-analysis"));

        JavaSparkContext sc = new JavaSparkContext(conf);
        return sc;
    }
}
