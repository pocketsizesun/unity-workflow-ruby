# frozen_string_literal: true

module Unity
  module Workflow
    class Recipe
      Step = Struct.new(:name, :handler)

      def initialize(name, unique: false)
        @name = name
        @steps = []
        @unique = unique
      end

      def step(name, handler = nil, &block)
        @steps << Step.new(name, handler || block)
      end

      def lock_key
        if @unique == true
          @name
        else
          "#{@name}:#{SecureRandom.uuid}"
        end
      end
    end
  end
end
