package spark.streaming.twitter;

import scala.Tuple2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class Utils {

    private static String format(int n) {
        return String.format("%2d", n);
    }

    private static String wrapScore(String s) {
        return String.format("[%s]", s);
    }

    private static String makeReadable(int n) {
        if (n > 0) return ConsoleColors.ANSI_GREEN + format(n) + ConsoleColors.ANSI_RESET;
        else if (n < 0) return ConsoleColors.ANSI_RED + format(n) + ConsoleColors.ANSI_RESET;
        else return format(n);
    }

    private static String makeReadable(String s) {
        return Arrays.toString(s.chars().takeWhile(x -> x != '\n').limit(80).toArray()) + "...";
    }

    public static String makeReadable(Tuple2<String,Integer> sn){
        return String.format("%s%s",wrapScore(makeReadable(sn._2)),makeReadable(sn._1));
    }

    public static Set<String> load(String file) throws IOException {
        InputStreamReader inputStreamReader = new InputStreamReader(Utils.class.getResourceAsStream(file));
        BufferedReader bf = new BufferedReader(inputStreamReader);
        Set<String> words = bf.lines().collect(Collectors.toSet());
        bf.close();
        return words;
    }

    public static List<String> wordsOf(String tweet) {
        return Arrays.asList(tweet.split(" "));
    }

    public static List<String> toLowerCase(List<String> list) {
        return list.stream()
                .map(String::toLowerCase)
                .collect(Collectors.toList());
    }

    public static List<String> keepActualWords(List<String> list) {
        return list.stream()
                .filter(word -> word.matches("[a-z]+"))
                .collect(Collectors.toList());
    }

    public static List<String> extractWords(List<String> list) {
        return list.stream()
                .map(String::toLowerCase)
                .filter(word -> word.matches("[a-z]+"))
                .collect(Collectors.toList());
    }

    public static List<String> keepMeaningfulWords(List<String> sentence, Set<String> useless) {
        return sentence.stream()
                .filter(x -> !useless.contains(x))
                .collect(Collectors.toList());
    }

    public static int computeScore(List<String> words, Set<String> positiveWords, Set<String> negativeWords) {
        return words.stream()
                .map(x -> computeWordScore(x, positiveWords, negativeWords))
                .collect(Collectors.summingInt(x -> Integer.valueOf(x)));
    }

    public static int computeWordScore(String word, Set<String> positiveWords, Set<String> negativeWords) {
        if (positiveWords.contains(word)) return 1;
        else if (negativeWords.contains(word)) return -1;
        else return 0;
    }

    public static String getSysParam(String name,String def){
        return System.getProperty(name, def);
    }

}
