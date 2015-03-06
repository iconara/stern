module Stern
  module Protocol
    class TopicOffsets
      attr_reader :topic_name, :partition_offsets

      def initialize(topic_name, partition_offsets)
        @topic_name = topic_name
        @partition_offsets = partition_offsets
      end
    end

    class PartitionOffset
      attr_reader :topic_name, :partition_id, :offset

      def initialize(topic_name, partition_id, offset)
        @topic_name = topic_name
        @partition_id = partition_id
        @offset = offset
      end

      def error?
        false
      end
    end

    class PartitionOffsetError < PartitionOffset
      attr_reader :error

      def initialize(topic_name, partition_id, error)
        super(topic_name, partition_id, nil)
        @error = error
      end

      def error?
        true
      end
    end

    class PartitionOffsets
      attr_reader :topic_name, :partition_id, :offsets

      def initialize(topic_name, partition_id, offsets)
        @topic_name = topic_name
        @partition_id = partition_id
        @offsets = offsets
      end

      def error?
        false
      end
    end

    class PartitionOffsetsError < PartitionOffsets
      attr_reader :error

      def initialize(topic_name, partition_id, error)
        super(topic_name, partition_id, [])
        @error = error
      end

      def error?
        true
      end
    end
  end
end
