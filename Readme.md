
# nsq_to_redis

> **Note**  
> Segment has paused maintenance on this project, but may return it to an active status in the future. Issues and pull requests from external contributors are not being considered, although internal contributions may appear from time to time. The project remains available under its open source license for anyone to use.

 Publish NSQ messages to Redis Publish/Subscribe:

```
$ nsq_to_redis --topic events --publish "projects:{projectId}"
```

 Write to capped lists:

```
$ nsq_to_redis --topic events --list "events:{projectId}" --list-size 100
```

# License

 MIT
