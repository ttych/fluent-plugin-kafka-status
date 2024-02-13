# frozen_string_literal: true

require 'helper'
require 'fluent/plugin/in_kafka_status'

class FakeBroker
  attr_reader :host

  def initialize(host)
    @host = host
  end
end

class FakeKafka
  def topics
    %w[default test]
  end

  def partitions_for(topic)
    return 2 if topic == 'default'
    return 3 if topic == 'test'

    1
  end

  def replica_count_for(topic)
    return 2 if topic == 'default'

    1
  end

  def brokers
    [FakeBroker.new('node1')]
  end
end

class KafkaStatusInputTest < Test::Unit::TestCase
  setup do
    Fluent::Test.setup
    @kafka = FakeKafka.new
  end

  ### configuration

  sub_test_case 'configuration' do
    test 'default configuration' do
      driver = create_driver
      input = driver.instance

      assert_equal Fluent::Plugin::KafkaStatusInput::DEFAULT_TAG, input.tag
      assert_equal Fluent::Plugin::KafkaStatusInput::DEFAULT_CLIENT_ID, input.client_id
      assert_equal Fluent::Plugin::KafkaStatusInput::DEFAULT_BROKERS, input.brokers
      assert_equal Fluent::Plugin::KafkaStatusInput::DEFAULT_INTERVAL, input.interval
      assert_equal Fluent::Plugin::KafkaStatusInput::DEFAULT_TIMESTAMP_FORMAT, input.timestamp_format
      assert_equal Fluent::Plugin::KafkaStatusInput::DEFAULT_EVENT_PREFIX, input.event_prefix
      assert_equal nil, input.connect_timeout
      assert_equal nil, input.ssl_ca_cert
      assert_equal nil, input.ssl_client_cert
      assert_equal nil, input.ssl_client_cert_key
      assert_equal true, input.ssl_verify_hostname
    end

    test 'tag can not be empty' do
      conf = %(
        #{BASE_CONF}
        tag
      )
      assert_raise(Fluent::ConfigError) do
        create_driver(conf)
      end
    end

    test 'hosts can not be empty' do
      conf = %(
        #{BASE_CONF}
        brokers
      )
      assert_raise(Fluent::ConfigError) do
        create_driver(conf)
      end
    end

    test 'ssl_ca_cert should be a valid file' do
      conf = %(
        #{BASE_CONF}
        ssl_ca_cert /nonexistent/file
      )
      assert_raise(Fluent::ConfigError) do
        create_driver(conf)
      end
    end

    test 'ssl_client_cert should be a valid file' do
      conf = %(
        #{BASE_CONF}
        ssl_client_cert /nonexistent/file
      )
      assert_raise(Fluent::ConfigError) do
        create_driver(conf)
      end
    end

    test 'ssl_client_cert_key should be a valid file' do
      conf = %(
        #{BASE_CONF}
        ssl_client_cert_key /nonexistent/file
      )
      assert_raise(Fluent::ConfigError) do
        create_driver(conf)
      end
    end
  end

  ### kafka_status
  sub_test_case 'kafka_status' do
    def test_emits_topics_status
      driver = create_driver
      mock_driver_kafka(driver)
      Timecop.freeze(MOCKED_TIME) do
        driver.instance.kafka_status
      end
      events = driver.events

      assert_equal 8, events.size
      assert_equal([
                     { 'metric_family' => 'topic',
                       'metric_name' => 'active',
                       'metric_value' => 1,
                       'timestamp' => '2024-01-01T00:00:00.000Z',
                       'topic' => 'default' },
                     { 'metric_family' => 'topic',
                       'metric_name' => 'configured_partitions_count',
                       'metric_value' => 2,
                       'timestamp' => '2024-01-01T00:00:00.000Z',
                       'topic' => 'default' },
                     { 'metric_family' => 'topic',
                       'metric_name' => 'configured_replica_count',
                       'metric_value' => 2,
                       'timestamp' => '2024-01-01T00:00:00.000Z',
                       'topic' => 'default' },
                     { 'metric_family' => 'topic',
                       'metric_name' => 'active',
                       'metric_value' => 1,
                       'timestamp' => '2024-01-01T00:00:00.000Z',
                       'topic' => 'test' },
                     { 'metric_family' => 'topic',
                       'metric_name' => 'configured_partitions_count',
                       'metric_value' => 3,
                       'timestamp' => '2024-01-01T00:00:00.000Z',
                       'topic' => 'test' },
                     { 'metric_family' => 'topic',
                       'metric_name' => 'configured_replica_count',
                       'metric_value' => 1,
                       'timestamp' => '2024-01-01T00:00:00.000Z',
                       'topic' => 'test' },
                     { 'metric_family' => 'cluster',
                       'metric_name' => 'brokers_count',
                       'metric_value' => 1,
                       'timestamp' => '2024-01-01T00:00:00.000Z' },
                     { 'metric_family' => 'cluster',
                       'metric_name' => 'broker_active',
                       'metric_value' => 1,
                       'tags_member_host' => 'node1',
                       'timestamp' => '2024-01-01T00:00:00.000Z' }
                   ],
                   events.map(&:last))
    end
  end

  private

  BASE_CONF = %()
  def create_driver(conf = BASE_CONF)
    Fluent::Test::Driver::Input.new(Fluent::Plugin::KafkaStatusInput).configure(conf)
  end

  MOCKED_TIME = Time.parse('2024-01-01T00:00:00.000Z')

  def mock_driver_kafka(driver)
    kafka = @kafka
    driver.instance.define_singleton_method(:new_kafka) do
      kafka
    end
  end
end
