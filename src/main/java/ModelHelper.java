import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;

public class ModelHelper {

    public static MatrixFactorizationModel loadModel(String path){
      return MatrixFactorizationModel.load(SparkContextWrapper.getContext().sc(),
                path);
    }

    public static void saveModel(String path,MatrixFactorizationModel model){
        model.save(SparkContextWrapper.getContext().sc(),path);
    }
}
