module Stern
  module Protocol
    class TopicMetadata
      attr_reader :topic_name, :partition_metadata

      def initialize(topic_name, partition_metadata)
        @topic_name = topic_name
        @partition_metadata = partition_metadata
      end

      def error?
        false
      end
    end

    class TopicMetadataError < TopicMetadata
      attr_reader :error

      def initialize(topic_name, error)
        super(topic_name, {})
        @error = error
      end

      def error?
        true
      end
    end

    class PartitionMetadata
      attr_reader :topic_name, :partition_id, :leader, :replicas, :isr

      def initialize(topic_name, partition_id, leader, replicas, isr)
        @topic_name = topic_name
        @partition_id = partition_id
        @leader = leader
        @replicas = replicas
        @isr = isr
      end

      def error?
        false
      end
    end

    class PartitionMetadataError < PartitionMetadata
      attr_reader :error

      def initialize(topic_name, partition_id, error)
        super(topic_name, partition_id, nil, {}, {})
        @error = error
      end

      def error?
        true
      end
    end
  end
end
