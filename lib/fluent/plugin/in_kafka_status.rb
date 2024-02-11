# frozen_string_literal: true

#
# Copyright 2024- Thomas Tych
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'fluent/plugin/input'

require 'kafka'

require_relative 'kafka_status_utils'
require_relative 'time_ext'

module Fluent
  module Plugin
    class KafkaStatusInput < Fluent::Plugin::Input
      NAME = 'kafka_status'
      Fluent::Plugin.register_input(NAME, self)

      include Fluent::Plugin::KafkaStatusUtils

      helpers :event_emitter, :timer

      DEFAULT_TAG = NAME
      DEFAULT_BROKERS = ['localhost:9092']
      DEFAULT_CLIENT_ID = "fluentd:#{NAME}"
      DEFAULT_INTERVAL = 3600
      DEFAULT_TIMESTAMP_FORMAT = :iso
      DEFAULT_EVENT_PREFIX = ''
      MAX_RETRIES = 3

      config_param :tag, :string, default: DEFAULT_TAG,
                                  desc: 'tag to emit status events on'
      config_param :brokers, :array, value_type: :string, default: DEFAULT_BROKERS,
                                     desc: 'kafka brokers: <host>:<port>,...'
      config_param :client_id, :string, default: DEFAULT_CLIENT_ID,
                                        desc: 'user agent like identifier for kafka connection'
      config_param :connect_timeout, :integer, default: nil,
                                               desc: 'timeout for connection to brokers'

      config_param :ssl_ca_cert, :string, default: nil,
                                          desc: 'a PEM encoded CA cert for SSL connection'
      config_param :ssl_client_cert, :string, default: nil,
                                              desc: 'a PEM encoded client cert for SSL connection'
      config_param :ssl_client_cert_key, :string, default: nil,
                                                  desc: 'a PEM encoded client cert key for SSL connection'
      config_param :ssl_verify_hostname, :bool, default: true,
                                                desc: 'whether hostname of certificate should be verified or not'

      config_param :interval, :time, default: DEFAULT_INTERVAL,
                                     desc: 'interval for the plugin execution'

      config_param :timestamp_format, :enum, list: %i[iso epochmillis], default: DEFAULT_TIMESTAMP_FORMAT,
                                             desc: 'timestamp format'
      config_param :event_prefix, :string, default: DEFAULT_EVENT_PREFIX,
                                           desc: 'Event prefix'

      def initialize
        super

        @kafka = nil
      end

      def configure(conf)
        super

        raise Fluent::ConfigError, 'tag should not be empty' if tag.nil? || tag.empty?
        raise Fluent::ConfigError, 'No brokers specified. Need one broker at least.' if brokers.empty?
        raise Fluent::ConfigError, 'ssl_ca_cert should be a file.' if ssl_ca_cert && !File.file?(ssl_ca_cert)

        raise Fluent::ConfigError, 'ssl_client_cert should be a file.' if ssl_client_cert && !File.file?(ssl_client_cert)
        raise Fluent::ConfigError, 'ssl_client_cert_key should be a file.' if ssl_client_cert_key && !File.file?(ssl_client_cert_key)
      end

      def start
        super

        timer_execute(:kafka_status_first, 1, repeat: false, &method(:kafka_status)) if interval > 60

        timer_execute(:kafka_status, interval, repeat: true, &method(:kafka_status))
      end

      def shutdown
        super
        close_kafka
      end

      def kafka_status
        events = []
        events += events_for_topics
        events += events_for_cluster_brokers
        time = Fluent::Engine.now
        events.each { |event| router.emit(tag, time, event) }
      end

      ### metric 1 - active topic
      # family: topic
      # name: active
      # value: 1
      # tags_topic: <topic>
      ### metric 2 - partitions_count
      # family: topic
      # name: configured_partitions_count
      # value: X
      # tags_topic: <topic>
      ### metric 3 - replica_count
      # family: topic
      # name: replica_count
      # value: count
      # tags_topic: <topic>
      def events_for_topics
        kafka do |kafka|
          events = []
          timestamp = Time.now.utc.send("to_#{timestamp_format}")
          kafka.topics.each do |topic|
            events
              .append({ 'timestamp' => timestamp,
                        'metric_family' => 'topic',
                        'metric_name' => 'active',
                        'metric_value' => 1,
                        "#{event_prefix}topic" => topic })
              .append({ 'timestamp' => timestamp,
                        'metric_family' => 'topic',
                        'metric_name' => 'configured_partitions_count',
                        'metric_value' => kafka.partitions_for(topic),
                        "#{event_prefix}topic" => topic })
              .append({ 'timestamp' => timestamp,
                        'metric_family' => 'topic',
                        'metric_name' => 'replica_count',
                        'metric_value' => kafka.replica_count_for(topic),
                        "#{event_prefix}topic" => topic })
          end
          events
        end
      end

      ### metric 4 - brokers_count
      # family: cluster
      # name: brokers_count
      # value: count
      # ???
      ### metric 5 - broker_active
      # family: cluster
      # name: broker_active
      # value: 1
      # tags_member_host: <host>
      # ???
      def events_for_cluster_brokers
        kafka do |kafka|
          events = []
          timestamp = Time.now.utc.send("to_#{timestamp_format}")
          brokers = kafka.brokers
          events.append({
                          'timestamp' => timestamp,
                          'metric_family' => 'cluster',
                          'metric_name' => 'brokers_count',
                          'metric_value' => brokers.size
                        })
          brokers.each do |broker|
            events.append({
                            'timestamp' => timestamp,
                            'metric_family' => 'cluster',
                            'metric_name' => 'broker_active',
                            'metric_value' => 1,
                            'tags_member_host' => broker.host
                          })
          end
          events
        end
      end

      def kafka(max_retries = MAX_RETRIES)
        retry_count = 0
        begin
          @kafka ||= new_kafka
          yield @kafka if block_given?
        rescue StandardError => e
          log.error "exception with kafka connection: #{e}"
          raise e if (retry_count += 1) > max_retries

          close_kafka
          sleep 30
          retry
        end
      end

      def new_kafka
        kafka = Kafka.new(
          seed_brokers: brokers,
          client_id: @client_id,
          connect_timeout: connect_timeout,
          ssl_ca_cert: read_file(ssl_ca_cert),
          ssl_client_cert: read_file(ssl_client_cert),
          ssl_client_cert_key: read_file(ssl_client_cert_key),
          ssl_ca_certs_from_system: !!ssl_ca_cert,
          ssl_verify_hostname: ssl_verify_hostname
        )
        log.info "initialized kafka producer: #{@client_id}"
        kafka
      end

      def close_kafka
        @kafka&.close
        @kafka = nil
      rescue StandardError
      end
    end
  end
end
