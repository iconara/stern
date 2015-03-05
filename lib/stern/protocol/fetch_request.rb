module Stern
  module Protocol
    class FetchRequest < Request
      def initialize(client_id, replica_id, max_wait_time, min_bytes, topic_fetches)
        super(1, client_id)
        @replica_id = replica_id
        @max_wait_time = max_wait_time
        @min_bytes = min_bytes
        @topic_fetches = topic_fetches
      end

      def to_b
        components = [@replica_id, @max_wait_time, @min_bytes, @topic_fetches.size]
        format = 'NNNN'
        @topic_fetches.each do |topic_name, partition_fetches|
          components << topic_name.bytesize
          components << topic_name
          components << partition_fetches.size
          format << 'na*N'
          partition_fetches.each do |pieces|
            components.concat(pieces)
            format << 'NQ>N'
          end
        end
        components.pack(format)
      end
    end
  end
end
