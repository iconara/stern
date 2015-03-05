module Stern
  module Protocol
    class MetadataRequest < Request
      def initialize(client_id, topics)
        super(3, client_id)
        @topics = topics
      end

      def to_b
        [@topics.size, *@topics.flat_map { |t| [t.bytesize, t] }].pack('N' << ('na*' * @topics.size))
      end
    end
  end
end
