package upf.edu.util;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

public class LanguageMapUtils {

    public static JavaPairRDD<String, String> buildLanguageMap(JavaRDD<String> lines) {
        JavaRDD<Tuple2<String, String>> result= lines.map(a -> lang_func(a));

        JavaPairRDD<String,String> pairs = JavaPairRDD.fromJavaRDD(result);
        return pairs;
    }

    public static Tuple2<String,String > lang_func(String l){
        String[] languages = l.split("[\t]");
        return  new Tuple2<String,String>(languages[1],languages[2]);
    }
}
