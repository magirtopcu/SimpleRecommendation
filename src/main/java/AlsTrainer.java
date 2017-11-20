import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import scala.Tuple2;

public class AlsTrainer {

    public AlsTrainResult train(String file,int rank,int numIteration){
        JavaSparkContext sparkContext = SparkContextWrapper.getContext();
        JavaRDD<String> fileToTrain = sparkContext.textFile(file);
        JavaRDD<Rating> convertedFileContent = convertFileToTrain(fileToTrain);
        MatrixFactorizationModel model = train(convertedFileContent,rank,numIteration);
        return evaluateModel(convertedFileContent,model);
    }

    public JavaRDD<Rating> convertFileToTrain(JavaRDD<String> fileContent){
        return fileContent.map(s->{
            String sarray[] = s.split("-");
            return new Rating(Integer.parseInt(sarray[0]),
                    Integer.parseInt(sarray[1]),
                    Double.parseDouble(sarray[2]));
        });


    }

    public MatrixFactorizationModel train(JavaRDD<Rating> ratingJavaRDD, int rank, int numIterations){
       return ALS.train(JavaRDD.toRDD(ratingJavaRDD), rank, numIterations, 0.01);
    }

    public AlsTrainResult evaluateModel(JavaRDD<Rating> ratingJavaRDD,MatrixFactorizationModel model){
        JavaRDD<Tuple2<Object, Object>> userProducts =
                ratingJavaRDD.map(r -> new Tuple2<>(r.user(), r.product()));
        JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions = JavaPairRDD.fromJavaRDD(
                model.predict(JavaRDD.toRDD(userProducts)).toJavaRDD()
                        .map(r -> new Tuple2<>(new Tuple2<>(r.user(), r.product()), r.rating()))
        );
        JavaRDD<Tuple2<Double, Double>> ratesAndPreds = JavaPairRDD.fromJavaRDD(
                ratingJavaRDD.map(r -> new Tuple2<>(new Tuple2<>(r.user(), r.product()), r.rating())))
                .join(predictions).values();
        double MSE = ratesAndPreds.mapToDouble(pair -> {
            double err = pair._1() - pair._2();
            return err * err;
        }).mean();

        AlsTrainResult result = new AlsTrainResult();
        result.errorMean= MSE;
        result.model = model;
        return  result;
    }


    public static  class AlsTrainResult {
        public    double errorMean;
        public     MatrixFactorizationModel model;
    }
}
