# frozen_string_literal: true

lib = File.expand_path('lib', __dir__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

Gem::Specification.new do |spec|
  spec.name    = 'fluent-plugin-kafka-status'
  spec.version = '0.2.0'
  spec.authors = ['Thomas Tych']
  spec.email   = ['thomas.tych@gmail.com']

  spec.summary       = 'fluentd plugin to extract status from kafka cluster'
  spec.homepage      = 'https://gitlab.com/ttych/fluent-plugin-kafka-status'
  spec.license       = 'Apache-2.0'

  spec.required_ruby_version = '>= 2.4.0'

  spec.metadata['rubygems_mfa_required'] = 'true'

  _, files = `git ls-files -z`.split("\x0").partition do |f|
    f.match(%r{^(test|spec|features)/})
  end
  spec.files         = files
  spec.executables   = files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.require_paths = ['lib']

  spec.add_development_dependency 'bump', '~> 0.10.0'
  spec.add_development_dependency 'bundler', '~> 2.5.6'
  spec.add_development_dependency 'byebug', '~> 11.1', '>= 11.1.3'
  spec.add_development_dependency 'rake', '~> 13.1.0'
  spec.add_development_dependency 'reek', '~> 6.1', '>= 6.1.4'
  spec.add_development_dependency 'rubocop', '~> 1.56'
  spec.add_development_dependency 'rubocop-rake', '~> 0.6.0'
  spec.add_development_dependency 'test-unit', '~> 3.6.1'
  spec.add_development_dependency 'timecop', '~> 0.9.6'

  spec.add_runtime_dependency 'fluentd', ['>= 0.14.10', '< 2']
  spec.add_runtime_dependency 'ruby-kafka', ['>= 1.3.0', '< 2']
end
