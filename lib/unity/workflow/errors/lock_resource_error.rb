# frozen_string_literal: true

module Unity
  module Workflow
    module Errors
      class LockResourceError < Error
        # @return [String]
        attr_reader :key

        # @param key [String]
        def initialize(key)
          super("Unable to create lock resource with key: #{key}")

          @key = key
        end
      end
    end
  end
end
