import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkContextWrapper {


    private  JavaSparkContext mJavaSparkContext;

    private static SparkContextWrapper mSparkContextWrapper;

    private SparkContextWrapper(){

    }

    private static synchronized SparkContextWrapper getInstance(){

        if(mSparkContextWrapper==null){
            mSparkContextWrapper = new SparkContextWrapper();
            SparkConf sparkConf = new SparkConf().setAppName("JavaALS").setMaster("local[2]").set("spark.executor.memory", "1g");
            JavaSparkContext sc = new JavaSparkContext(sparkConf);
            mSparkContextWrapper.setJavaSparkContext(sc);
        }
        return mSparkContextWrapper;
    }

    private  void setJavaSparkContext(JavaSparkContext context){
        mJavaSparkContext = context;
    }

    public static JavaSparkContext getContext(){
        return  getInstance().mJavaSparkContext;
    }
}
