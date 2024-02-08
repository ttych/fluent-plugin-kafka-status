# fluent-plugin-kafka-status

[Fluentd](https://fluentd.org/) input plugin for *kafka* status.


## plugins

### in - kafka_status

Poll kafka cluster status.

#### configuration

Example:

``` conf
<source>
  @type kafka_status
  tag kafka_status
</source>
```

Options are:
* tag: Tag to emit events on
* brokers: list of borkers <host>:<port>,...
* connect_timeout: timeout for connecting to brokers
* ssl_ca_cert: CA cert to use for SSL connection
* ssl_client_cert: client cert for the SSL connection
* ssl_client_cert_key: client key for the SSL connection
* ssl_verify_hostname: verify certificate hostname
* interval: interval between execution
* timestamp_format: iso, epochmillis timestamp format (iso)
* event_prefix: metric event prefix for extra dimension

#### output

Generated metrics are :



## Installation

Manual install, by executing:

    $ gem install fluent-plugin-kafka-status

Add to Gemfile with:

    $ bundle add fluent-plugin-kafka-status


## Compatibility

should work with:
- ruby >= 2.4.0
- ruby-kafka >= 1.3.0


## Copyright

* Copyright(c) 2024- Thomas Tych
* License
  * Apache License, Version 2.0
