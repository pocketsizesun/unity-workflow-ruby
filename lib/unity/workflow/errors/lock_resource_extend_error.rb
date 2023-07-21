# frozen_string_literal: true

module Unity
  module Workflow
    module Errors
      class LockResourceExtendError < Error
        # @return [Unity::Workflow::LockResource]
        attr_reader :lock_resource

        # @param lock_resource [String]
        def initialize(lock_resource)
          super("Unable to extend lock key: #{lock_resource.key}")

          @lock_resource = lock_resource
        end
      end
    end
  end
end
