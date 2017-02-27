# Custom Twitter Stream and Analysis with Apache Flink
This repository is meant to show how implement Twitter streaming with Apache Flink. By default Apache Flink gets all Tweets in real time, however by implementing the EndPointInitializer you can get just the relevant tweets. For instance, you would do something like the following
```Java 
List relevantWords<String> = new ArrayList<Strings>;
relevantWords.add("politics");
List<Location> relevantLocations = new ArrayList<Location>;
List<Long> userIDS = new ArrayList<Long>;
Filter f = new Filter(relevantWords,relevantLocations,relevantPeople);
//Alternatively you could also pass null
Filter f = new Filter(relevantWords,null,null);
```
Check Filter.java to see how to this works. Over the next few weeks we will be adding more details as well.
**More info to come soon**
