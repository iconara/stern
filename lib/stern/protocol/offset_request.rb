module Stern
  module Protocol
    class OffsetRequest < Request
      attr_reader :replica_id

      def initialize(client_id, replica_id, topic_offset_queries)
        super(2, client_id)
        @replica_id = replica_id
        @topic_offset_queries = topic_offset_queries
      end

      def to_b
        components = [@replica_id, @topic_offset_queries.size]
        format = 'NN'
        @topic_offset_queries.each do |topic_name, partition_offset_queries|
          components << topic_name.bytesize
          components << topic_name
          components << partition_offset_queries.size
          format << 'na*N'
          partition_offset_queries.each do |pieces|
            components.concat(pieces)
            format << 'NQ>N'
          end
        end
        components.pack(format)
      end
    end
  end
end
