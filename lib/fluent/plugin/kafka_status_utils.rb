## frozen_string_literal: true

module Fluent
  module Plugin
    module KafkaStatusUtils
      def read_file(path)
        return nil if path.nil?
        File.read(path)
      end
    end
  end
end
