module Stern
  module Protocol
    class Request
      attr_reader :api_key, :client_id

      def initialize(api_key, client_id)
        @api_key = api_key
        @client_id = client_id
      end

      def to_b
      end
    end
  end
end
