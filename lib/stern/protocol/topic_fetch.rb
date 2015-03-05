module Stern
  module Protocol
    class TopicFetch
      attr_reader :topic_name, :partition_fetches

      def initialize(topic_name, partition_fetches)
        @topic_name = topic_name
        @partition_fetches = partition_fetches
      end
    end

    class PartitionFetch
      attr_reader :topic_name, :partition_id, :hwm_offset, :message_set

      def initialize(topic_name, partition_id, hwm_offset, message_set)
        @topic_name = topic_name
        @partition_id = partition_id
        @hwm_offset = hwm_offset
        @message_set = message_set
      end

      def error?
        false
      end
    end

    class PartitionFetchError < PartitionFetch
      attr_reader :error

      def initialize(topic_name, partition_id, error)
        super(topic_name, partition_id, nil, nil)
        @error = error
      end

      def error?
        true
      end
    end
  end
end
