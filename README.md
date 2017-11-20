# SimpleRecommendation
Simple recommendation application with Apache Spark as using Alternating Least Squares  - Collabrative Filtering
[![Build Status](https://travis-ci.org/magirtopcu/SimpleRecommendation.svg?branch=master)](https://travis-ci.org/magirtopcu/SimpleRecommendation)
***

### Train Dataset
```
trainer.train(dataset,...);
```
### And Recommend
```
Rating[] ratings = model.recommendProducts(userId,numProducts);
```

[Full example](https://github.com/magirtopcu/SimpleRecommendation/blob/master/src/main/java/Main.java)
