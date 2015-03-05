module Stern
  module Protocol
    class MetadataResponse < Response
      attr_reader :topic_metadata

      def initialize(topic_metadata)
        @topic_metadata = topic_metadata
      end

      def self.decode(bytes)
        buffer = KafkaByteBuffer.new(bytes)
        brokers = read_brokers(buffer)
        topics = read_topic_metadata(buffer, brokers)
        new(topics)
      end

      private

      def self.read_brokers(buffer)
        brokers = {}
        buffer.read_array do
          node_id = buffer.read_int
          host = buffer.read_string
          port = buffer.read_int
          brokers[node_id] = Broker.new(node_id, host, port)
        end
        brokers
      end

      def self.read_topic_metadata(buffer, brokers)
        topics = {}
        buffer.read_array do
          error_code = buffer.read_short
          topic_name = buffer.read_string
          partition_metadata = read_partition_metadata(buffer, brokers, topic_name)
          if error_code == 0
            topics[topic_name] = TopicMetadata.new(topic_name, partition_metadata)
          else
            topics[topic_name] = TopicMetadataError.new(topic_name, Errors[error_code].new)
          end
        end
        topics
      end

      def self.read_partition_metadata(buffer, brokers, topic_name)
        partitions = {}
        buffer.read_array do
          error_code = buffer.read_short
          partition_id = buffer.read_int
          leader = buffer.read_int
          replicas = buffer.read_array { brokers[buffer.read_int] }
          isr = buffer.read_array { brokers[buffer.read_int] }
          if error_code == 0
            partitions[partition_id] = PartitionMetadata.new(topic_name, partition_id, brokers[leader], replicas, isr)
          else
            partitions[partition_id] = PartitionMetadataError.new(topic_name, partition_id, Errors[error_code].new)
          end
        end
        partitions
      end
    end
  end
end
