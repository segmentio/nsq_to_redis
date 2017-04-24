
v2.0.0 / 2017-04-24
===================

  * Speed up JSON parsing by using `json.RawMessage` instead of `map[string]interface{}`
  * Speed up templating by writing custom templating on top of [`gjson`](https://github.com/tidwall/gjson) instead of using [`go-interpolate`](https://github.com/segmentio/go-interpolate).
  * Internal: Migrate to govendor.


v1.5.0 / 2016-01-29
==================

  * add ratelimit
  * list: typo
  * add Message


v1.4.0 / 2016-01-25
==================

  * Flush once for each message


v1.3.0 / 2016-01-22
==================

  * add --max-idle

v1.2.0 / 2016-01-22
==================

  * add optional metrics (see `--statsd` & `--statsd-prefix`)
  * fixing nsqlookupd detection

v1.1.0 / 2015-08-25
==================

  * add nsqds argument that overrides nsqlookupd


v1.0.2 / 2015-05-06
===================

  * use a different redis lib, seems less buggy

v1.0.0 / 2015-05-06
===================

  * add --idle-timeout

v0.2.0 / 2015-02-19
===================

  * add Broadcast to deliver to delegates

v0.1.0 / 2015-02-19
===================

  * add capped list support
  * add --list-size
  * add --list
  * rename Relay to PubSub

v0.0.2 / 2015-02-18
===================

 - update go-interpolate for trailing lit bugfix
