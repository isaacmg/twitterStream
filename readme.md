# Custom Twitter Stream and Analysis with Apache Flink

[![Build Status](https://travis-ci.org/isaacmg/twitterStream.svg?branch=master)](https://travis-ci.org/isaacmg/twitterStream)

This repository is meant to show how to implement Twitter streaming with Apache Flink and output tweets to KafkaTableSink. By default Apache Flink gets all Tweets in real time, however by implementing the EndPointInitializer you can get just the relevant tweets. This is what Filter.java does. To use Filter.java for instance, you would do something like the following
```Java 
List relevantWords<String> = new ArrayList<Strings>;
relevantWords.add("politics");
List<Location> relevantLocations = new ArrayList<Location>;
List<Long> userIDS = new ArrayList<Long>;
Filter f = new Filter(relevantWords,relevantLocations,relevantPeople);
// Alternatively you could also pass null
Filter f = new Filter(relevantWords,null,null);
```
 Feel free to reach out if you have any questions/issues.
 
[Article on using Flink to build a dashboard](https://medium.com/towards-data-science/building-a-realtime-dashboard-with-flink-the-backend-a9393062de92)

**More info to come soon**
