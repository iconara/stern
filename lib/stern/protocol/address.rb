module Stern
  module Protocol
    class Address
      include Utils::FnvHash

      attr_reader :host, :port

      def initialize(host, port)
        @host = host
        @port = port
      end

      def eql?(other)
        !other.nil? && @host.eql?(other.host) && @port.eql?(other.port)
      end
      alias_method :==, :eql?

      def hash
        fnv_hash(@host, @port)
      end
    end
  end
end
