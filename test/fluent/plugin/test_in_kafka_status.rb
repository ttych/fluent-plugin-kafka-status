# frozen_string_literal: true

require 'helper'
require 'fluent/plugin/in_kafka_status'

class KafkaStatusInputTest < Test::Unit::TestCase
  setup do
    Fluent::Test.setup
  end

  test 'failure' do
    true
  end

  private

  def create_driver(conf)
    Fluent::Test::Driver::Input.new(Fluent::Plugin::KafkaStatusInput).configure(conf)
  end
end
