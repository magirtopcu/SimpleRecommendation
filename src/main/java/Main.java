import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;

import java.io.IOException;

public class Main {

    public static void main(String args[]) throws IOException {
        AlsTrainer trainer = new AlsTrainer();
        AlsTrainer.AlsTrainResult result = trainer.train("file://"+Main.class.getResource("dataset.data").getPath(),10,100);

        System.out.println("Train error => " + result.errorMean);
        String modelPath = "target/tmp/collabrativeFilterModel";
        ModelHelper.saveModel(modelPath,result.model);

        MatrixFactorizationModel model  = ModelHelper.loadModel(modelPath);

        Rating[] ratings = model.recommendProducts(34,10);

        StringBuffer buffer = new StringBuffer();
        for(Rating r : ratings){
            buffer.append(getItemTxt(r)).append("<br>\n");
        }
        System.out.println("predicts : \n "+ buffer.toString());

    }

    public  static String getItemTxt(Rating r){
        return  r.user() +" -> "+ r.product()+ " -> "+r.rating();
    }
}
