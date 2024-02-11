# frozen_string_literal: true

require 'helper'
require 'fluent/plugin/time_ext'

class TimeTest < Test::Unit::TestCase
  setup do
    Fluent::Test.setup
  end

  sub_test_case '.to_epochmillis' do
    test 'it formats to epochmillis format' do
      assert_equal 1_672_531_261_000, Time.parse('2023-01-01T01:01:01').to_epochmillis
    end
  end

  sub_test_case '.to_iso' do
    test 'it formats to iso8601 format' do
      assert_equal '2023-01-01T01:01:01.000+01:00', Time.parse('2023-01-01T01:01:01').to_iso
    end
  end
end
