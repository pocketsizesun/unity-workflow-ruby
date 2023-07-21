# frozen_string_literal: true

module Unity
  module Workflow
    class LockResource
      # @return [Unity::Workflow::Client]
      attr_reader :client

      # @return [String]
      attr_reader :key

      # @return [String]
      attr_reader :id

      def initialize(client, key, id)
        @client = client
        @key = key
        @id = id
      end

      def extend_ttl(ttl)
        @client.extend_lock(key, id)
      end
    end
  end
end
