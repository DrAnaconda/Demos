## Intorduction

Simple code pieces to demonstrate coding style, methodology, etc.

Dependencies are not included, for read only / evaluation purposes only

## AccessHelperDemo.cs

Simplified security library (permission checker) based on bitmasks operations

## HealthCheckDemo.cs

Customizable CPU health check for API backend node. Just set up thresholds and deploy

## MongoDBRepositoryDemo.cs

Implementation of `Repository` pattern. Derived from `BaseMongoRepo` class which have shortcuts to access mongo DB collection.

How to use:
1. Derive `BaseMongoRepo`
2. Add public const string with collection name to use in `getCollection` method
3. Override `getCollection` method
4. Use const in aggregators or `getCollection` method
5. Add custom queries to Repository, if needed

Architecture:
Controller => Repository => Aggregator

## MongoDBAggregatorDemo.cs

Example of aggregator

## MongoDBWatcherDemo.cs

Implementation of collection watcher. Usually used for notification generation.

Current implementation receives all changes in collection (update, insert, delete).

This implementation is scalable. You can use your own thread / microservice to handle every type of changes.

How to use:
1. Derive `NotificationBaseWatcher`
2. Use method `configureWatcher`
3. Implement logic
4. Override `Dispose` method