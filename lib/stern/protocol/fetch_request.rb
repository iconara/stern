module Stern
  module Protocol
    class FetchRequest < Request
      def initialize(client_id, replica_id, max_wait_time, min_bytes, fetches)
        super(1, client_id)
        @replica_id = replica_id
        @max_wait_time = max_wait_time
        @min_bytes = min_bytes
        @topic_fetches = fetches.group_by { |f| f.topic_name }
      end

      def to_b
        components = [@replica_id, @max_wait_time, @min_bytes, @topic_fetches.size]
        format = 'NNNN'
        @topic_fetches.each_value do |fetches|
          topic_name = fetches.first.topic_name
          components << topic_name.bytesize
          components << topic_name
          components << fetches.size
          format << 'na*N'
          fetches.each do |fetch|
            components << fetch.partition_id
            components << fetch.offset
            components << fetch.max_bytes
            format << 'NQ>N'
          end
        end
        components.pack(format)
      end

      class Fetch
        attr_reader :topic_name, :partition_id, :offset, :max_bytes

        def initialize(topic_name, partition_id, offset, max_bytes)
          @topic_name = topic_name
          @partition_id = partition_id
          @offset = offset
          @max_bytes = max_bytes
        end
      end
    end
  end
end
