module Stern
  module Protocol
    class ProduceRequest < Request
      def initialize(client_id, required_acks, timeout, topic_message_sets)
        super(0, client_id)
        @required_acks = required_acks
        @timeout = timeout
        @topic_message_sets = topic_message_sets
      end

      def to_b
        components = [@required_acks, @timeout, @topic_message_sets.size]
        format = 'nNN'
        @topic_message_sets.each do |topic, partition_message_sets|
          components << topic.bytesize
          components << topic
          components << partition_message_sets.size
          format << 'na*N'
          partition_message_sets.each do |partition_id, message_set|
            message_set_bytes = message_set.to_b
            components << partition_id
            components << message_set_bytes.bytesize
            components << message_set_bytes
            format << 'NNa*'
          end
        end
        components.pack(format)
      end
    end
  end
end
