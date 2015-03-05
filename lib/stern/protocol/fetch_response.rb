module Stern
  module Protocol
    class FetchResponse < Response
      attr_reader :topic_fetches

      def initialize(topic_fetches)
        @topic_fetches = topic_fetches
      end

      def self.decode(bytes)
        buffer = KafkaByteBuffer.new(bytes)
        topic_fetches = read_topic_fetches(buffer)
        new(topic_fetches)
      end

      def self.merge(*fetch_responses)
        all_partition_fetches = []
        fetch_responses.each do |fetch_response|
          fetch_response.topic_fetches.each_value do |topic_fetch|
            all_partition_fetches.concat(topic_fetch.partition_fetches.values)
          end
        end
        groups = all_partition_fetches.group_by { |pf| pf.topic_name }
        groups.merge!(groups) do |topic_name, partition_fetches|
          pfs = partition_fetches.each_with_object({}) do |pf, pfs|
            pfs[pf.partition_id] = pf
          end
          TopicFetch.new(topic_name, pfs)
        end
        new(groups)
      end

      private

      def self.read_topic_fetches(buffer)
        topic_fetches = {}
        buffer.read_array do
          topic_name = buffer.read_string
          partition_fetches = read_partition_fetches(buffer, topic_name)
          topic_fetches[topic_name] = TopicFetch.new(topic_name, partition_fetches)
        end
        topic_fetches
      end

      def self.read_partition_fetches(buffer, topic_name)
        partition_fetches = {}
        buffer.read_array do
          partition_id = buffer.read_int
          error_code = buffer.read_short
          hwm_offset = buffer.read_long
          message_set = buffer.read_bytes
          if error_code == 0
            partition_fetches[partition_id] = PartitionFetch.new(topic_name, partition_id, hwm_offset, EncodedMessageSet.new(message_set))
          else
            partition_fetches[partition_id] = PartitionFetchError.new(topic_name, partition_id, Errors[error_code].new)
          end
        end
        partition_fetches
      end
    end
  end
end
