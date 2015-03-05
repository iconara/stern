module Stern
  module Protocol
    class AddressedRequest < Request
      attr_reader :address

      def initialize(request, address)
        super(request.api_key, request.client_id)
        @request = request
        @address = address
      end

      def to_b
        @request.to_b
      end
    end
  end
end
