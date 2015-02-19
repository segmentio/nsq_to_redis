
# nsq_to_redis

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