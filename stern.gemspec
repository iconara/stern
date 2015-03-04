# encoding: utf-8

$: << File.expand_path('../lib', __FILE__)

require 'stern/version'


Gem::Specification.new do |s|
  s.name          = 'stern'
  s.version       = Stern::VERSION.dup
  s.authors       = ['Theo Hultberg']
  s.email         = ['theo@iconara.net']
  s.homepage      = 'http://github.com/iconara/stern'
  s.summary       = %q{}
  s.description   = %q{}
  s.license       = 'Apache License 2.0'

  s.files         = Dir['lib/**/*.rb', 'README.md', '.yardopts']
  s.require_paths = %w(lib)

  s.add_dependency 'ione-rpc', '~> 1.0'

  s.platform = Gem::Platform::RUBY
  s.required_ruby_version = '>= 1.9.3'
end
