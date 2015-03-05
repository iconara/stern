require 'spec_helper'

module Stern
  module Protocol
    describe AddressedRequest do
      let :request do
        double(:request, client_id: 'the client', api_key: 3)
      end

      let :address do
        double(:address)
      end

      describe '#to_b' do
        it 'forwards the call to the wrapped request' do
          allow(request).to receive(:to_b).and_return("\x0f\x0b\xa5")
          bytes = described_class.new(request, address).to_b
          expect(bytes).to eq("\x0f\x0b\xa5")
        end
      end
    end
  end
end
