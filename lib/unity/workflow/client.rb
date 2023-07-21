# frozen_string_literal: true

module Unity
  module Workflow
    class Client
      LOCK_UPDATE_EXPRESSION = 'SET w = :w, lid = :lid, e = :e'
      LOCK_KEY_FORMAT = '%s/l/%s'
      VALUE_KEY_FORMAT = '%s/v/%s'

      # @param namespace [String] Key namespace
      # @param table_name [String] Table name to store locks and values
      # @param aws_dynamodb_client [Aws::DynamoDB::Client] DynamoDB client to use
      # @param worker_id [String] Set worker ID (optional)
      # @param lock_default_ttl [String] Set default lock TTL (default: 60)
      # @param value_default_ttl [String] Set default value TTL (default: 60)
      # @param consistent_reads [Boolean] Use DynamoDB consistent reads mode (default: true)
      def initialize(namespace, table_name, **kwargs)
        @namespace = namespace
        @table_name = table_name
        @worker_id = kwargs[:worker_id] || SecureRandom.uuid
        @aws_dynamodb_client = kwargs[:aws_dynamodb_client] || Aws::DynamoDB::Client.new
        @lock_default_ttl = kwargs[:lock_default_ttl] || 60
        @value_default_ttl = kwargs[:value_default_ttl] || 60
        @consistent_reads = kwargs[:consistent_reads] || true
      end

      # @param key [String]
      # @param value [Object]
      # @param ttl [Integer, nil]
      # @return [Boolean]
      def store(key, value, ttl: nil)
        @aws_dynamodb_client.put_item(
          table_name: @table_name,
          item: {
            'k' => format(VALUE_KEY_FORMAT, @namespace, key),
            'v' => value,
            'e' => Process.clock_gettime(Process::CLOCK_REALTIME, :second) + (ttl || @value_default_ttl)
          }
        )

        true
      end

      # @param key [String]
      # @return [Object, nil]
      def fetch(key, default_value = nil)
        get_item_parameters = {
          table_name: @table_name,
          projection_expression: 'v, e',
          key: { 'k' => format(VALUE_KEY_FORMAT, @namespace, key) },
          consistent_read: @consistent_reads
        }
        result = @aws_dynamodb_client.get_item(get_item_parameters)
        return default_value if result.item.nil?

        if result.item['e'].to_i <= Process.clock_gettime(Process::CLOCK_REALTIME, :second)
          return default_value
        end

        result.item['v']
      rescue Aws::DynamoDB::Errors::ConditionalCheckFailedException
        return default_value
      end

      # @param key [String]
      # @param ttl [Integer, nil]
      # @param sleep_time [Integer, Float]
      # @yieldparam [Unity::Workflow::LockResource]
      # @return [Object]
      def wait_and_lock(key, ttl: nil, sleep_time: 1, &block)
        with_lock(key, ttl: ttl, tries: nil, sleep_time: sleep_time, &block)
      end

      # @param key [String]
      # @param ttl [Integer, nil]
      # @param tries [Integer, nil]
      # @param sleep_time [Integer, Float]
      # @yieldparam [Unity::Workflow::LockResource]
      # @return [Object]
      def with_lock(key, ttl: nil, tries: 1, sleep_time: 1, &_block)
        lock_resource = nil
        retries_count = 0

        begin
          lock_resource = lock(key, ttl: ttl)
        rescue Unity::Workflow::Errors::LockResourceError => e
          if tries.nil? || retries_count < tries
            retries_count += 1
            sleep(sleep_time)
            retry
          end

          raise e
        end

        begin
          retval = yield(lock_resource)
        ensure
          release(lock_resource)
        end

        retval
      end

      # @param key [String]
      # @param ttl [Integer, nil]
      # @raise [Unity::Workflow::Errors::LockResourceError]
      # @return [Unity::Workflow::LockResource]
      def lock(key, ttl: nil)
        current_time = Process.clock_gettime(Process::CLOCK_REALTIME, :second)
        lock_id = SecureRandom.uuid

        @aws_dynamodb_client.update_item(
          table_name: @table_name,
          key: { 'k' => format(LOCK_KEY_FORMAT, @namespace, key) },
          expression_attribute_values: {
            ':w' => @worker_id,
            ':lid' => lock_id,
            ':tn' => current_time,
            ':e' => current_time + (ttl || @lock_default_ttl)
          },
          condition_expression: 'attribute_not_exists(k) OR w = :w OR e <= :tn',
          update_expression: LOCK_UPDATE_EXPRESSION
        )

        Unity::Workflow::LockResource.new(self, key, lock_id)
      rescue Aws::DynamoDB::Errors::ConditionalCheckFailedException
        raise Unity::Workflow::Errors::LockResourceError.new(key)
      end

      # @param lock_resource [Unity::Workflow::LockResource]
      # @raise [Unity::Workflow::Errors::LockResourceExtendError]
      # @return [Boolean]
      def extend_lock(lock_resource)
        @aws_dynamodb_client.update_item(
          table_name: @table_name,
          key: { 'k' => format(LOCK_KEY_FORMAT, @namespace, lock_resource.key) },
          expression_attribute_values: {
            ':w' => @worker_id,
            ':lid' => lock_resource.id,
            ':e' => current_time + (ttl || @lock_default_ttl)
          },
          condition_expression: 'w = :w AND lid = :lid',
          update_expression: 'SET e = :e'
        )

        true
      rescue Aws::DynamoDB::Errors::ConditionalCheckFailedException
        raise Unity::Workflow::Errors::LockResourceExtendError.new(lock_resource)
      end

      # @param lock_resource [Unity::Workflow::LockResource]
      # @return [Boolean]
      def release(lock_resource)
        @aws_dynamodb_client.delete_item(
          table_name: @table_name,
          key: { 'k' => format(LOCK_KEY_FORMAT, @namespace, lock_resource.key) },
          condition_expression: 'lid = :lid',
          expression_attribute_values: { ':lid' => lock_resource.id }
        )

        true
      rescue Aws::DynamoDB::Errors::ConditionalCheckFailedException
        false
      end
    end
  end
end
