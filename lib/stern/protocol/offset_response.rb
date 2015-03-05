module Stern
  module Protocol
    class OffsetResponse < Response
      attr_reader :topic_offsets

      def initialize(topic_offsets)
        @topic_offsets = topic_offsets
      end

      def self.decode(bytes)
        buffer = KafkaByteBuffer.new(bytes)
        topic_offsets = read_topic_offsets(buffer)
        new(topic_offsets)
      end

      def self.merge(*offset_responses)
        all_partition_offsets = []
        offset_responses.each do |offset_response|
          offset_response.topic_offsets.each_value do |topic_offsets|
            all_partition_offsets.concat(topic_offsets.partition_offsets.values)
          end
        end
        groups = all_partition_offsets.group_by { |po| po.topic_name }
        groups.merge!(groups) do |topic_name, partition_offsets|
          pos = partition_offsets.each_with_object({}) do |po, pos|
            pos[po.partition_id] = po
          end
          TopicOffsets.new(topic_name, pos)
        end
        new(groups)
      end

      private

      def self.read_topic_offsets(buffer)
        topics = {}
        buffer.read_array do
          topic_name = buffer.read_string
          partition_offsets = read_partition_offsets(buffer, topic_name)
          topics[topic_name] = TopicOffsets.new(topic_name, partition_offsets)
        end
        topics
      end

      def self.read_partition_offsets(buffer, topic_name)
        partitions = {}
        buffer.read_array do
          partition_id = buffer.read_int
          error_code = buffer.read_short
          offsets = buffer.read_array { buffer.read_long }
          if error_code == 0
            partitions[partition_id] = PartitionOffsets.new(topic_name, partition_id, offsets)
          else
            partitions[partition_id] = PartitionOffsetsError.new(topic_name, partition_id, Errors[error_code].new)
          end
        end
        partitions
      end
    end
  end
end
