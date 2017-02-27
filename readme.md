# Custom Twitter Stream and Analysis with Apache Flink
This repository is meant to show how to implement Twitter streaming with Apache Flink. By default Apache Flink gets all Tweets in real time, however by implementing the EndPointInitializer you can get just the relevant tweets. This is what Filter.java does. To use Filter.java for instance, you would do something like the following
```Java 
List relevantWords<String> = new ArrayList<Strings>;
relevantWords.add("politics");
List<Location> relevantLocations = new ArrayList<Location>;
List<Long> userIDS = new ArrayList<Long>;
Filter f = new Filter(relevantWords,relevantLocations,relevantPeople);
//Alternatively you could also pass null
Filter f = new Filter(relevantWords,null,null);
```
Over the next few weeks I will be adding more details and some more examples. Feel free to reach out if you have any questions/issues.
**More info to come soon**
