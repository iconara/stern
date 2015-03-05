module Stern
  module Protocol
    class Broker
      include Utils::FnvHash

      attr_reader :node_id, :host, :port

      def initialize(node_id, host, port)
        @node_id = node_id
        @host = host
        @port = port
      end

      def eql?(other)
        !other.nil? && @node_id.eql?(other.node_id) && @host.eql?(other.host) && @port.eql?(other.port)
      end
      alias_method :==, :eql?

      def hash
        @hash ||= fnv_hash(@node_id, @host, @port)
      end
    end
  end
end
