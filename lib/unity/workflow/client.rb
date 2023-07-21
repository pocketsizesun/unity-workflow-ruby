# frozen_string_literal: true

module Unity
  module Workflow
    class Client
      LOCK_UPDATE_EXPRESSION = 'SET w = :w, lid = :lid, e = :e'
      LOCK_KEY_FORMAT = '%s/l/%s'
      VALUE_KEY_FORMAT = '%s/v/%s'

      # @param aws_dynamodb_client [Aws::DynamoDB::Client]
      def initialize(namespace, table_name, **kwargs)
        @namespace = namespace
        @table_name = table_name
        @worker_id = kwargs[:worker_id] || SecureRandom.uuid
        @aws_dynamodb_client = kwargs[:aws_dynamodb_client] || Aws::DynamoDB::Client.new
        @lock_default_ttl = kwargs[:lock_default_ttl] || 60
        @consistent_reads = kwargs[:consistent_reads] || true
      end

      def store(key, value, ttl: nil)
        @aws_dynamodb_client.put_item(
          table_name: @table_name,
          item: {
            'k' => format(VALUE_KEY_FORMAT, @namespace, key),
            'v' => value,
            'e' => Process.clock_gettime(Process::CLOCK_REALTIME, :second) + (ttl || @lock_default_ttl)
          }
        )

        true
      end

      def fetch(key, ignore_ttl: false)
        get_item_parameters = {
          table_name: @table_name,
          projection_expression: 'v, e',
          key: { 'k' => format(VALUE_KEY_FORMAT, @namespace, key) },
          consistent_read: @consistent_reads
        }
        result = @aws_dynamodb_client.get_item(get_item_parameters)
        return nil if result.item.nil?

        if ignore_ttl == false &&
           result.item['e'].to_i <= Process.clock_gettime(Process::CLOCK_REALTIME, :second)
          return nil
        end

        result.item['v']
      rescue Aws::DynamoDB::Errors::ConditionalCheckFailedException
        return nil
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
      def lock(key, ttl: nil)
        current_time = Process.clock_gettime(Process::CLOCK_REALTIME, :second)
        lock_id = SecureRandom.uuid

        @aws_dynamodb_client.update_item(
          table_name: @table_name,
          key: { 'k' => format(LOCK_KEY_FORMAT, @namespace, lock_resource.key) },
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
      rescue Aws::DynamoDB::Errors::ConditionalCheckFailedException
        raise Unity::Workflow::Errors::LockResourceExtendError.new(lock_resource)
      end

      def release(lock_resource)
        @aws_dynamodb_client.delete_item(
          table_name: @table_name,
          key: { 'k' => format(LOCK_KEY_FORMAT, @namespace, lock_resource.key) },
          condition_expression: 'lid = :lid',
          expression_attribute_values: { ':lid' => lock_resource.id }
        )

        true
      rescue Aws::DynamoDB::Errors::ConditionalCheckFailedException
        true
      end
    end
  end
end
