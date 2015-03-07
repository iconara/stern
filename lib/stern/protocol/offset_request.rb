module Stern
  module Protocol
    attr_reader :replica_id

    class OffsetRequest < Request
      def initialize(client_id, replica_id, queries)
        super(2, client_id)
        @replica_id = replica_id
        @topic_queries = queries.group_by { |q| q.topic_name }
      end

      def to_b
        components = [@replica_id, @topic_queries.size]
        format = 'NN'
        @topic_queries.each_value do |queries|
          topic_name = queries.first.topic_name
          components << topic_name.bytesize
          components << topic_name
          components << queries.size
          format << 'na*N'
          queries.each do |query|
            components << query.partition_id
            components << query.timestamp
            components << query.max_offsets
            format << 'NQ>N'
          end
        end
        components.pack(format)
      end

      class Query
        attr_reader :topic_name, :partition_id, :timestamp, :max_offsets

        def initialize(topic_name, partition_id, timestamp, max_offsets)
          @topic_name = topic_name
          @partition_id = partition_id
          @timestamp = timestamp
          @max_offsets = max_offsets
        end
      end
    end
  end
end
