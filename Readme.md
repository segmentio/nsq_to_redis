
# nsq_to_redis

 Publish NSQ messages to Redis PUB/SUB.

```
$ nsq_to_redis --topic events --publish "projects:{projectId}"
```

## Usage

 Messages are published to Redis according to the `--publish` template provided. Only JSON messages are currently supported, and if the path(s) in the template are not present the publish will fail.

 For example suppose your messages have a `userId`, you may want to publish with `--publish "users:{userId}"`, or by nested properties: `--publish "location:{location.city}"`.

# License

 MIT