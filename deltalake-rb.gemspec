require_relative "lib/deltalake/version"

Gem::Specification.new do |spec|
  spec.name          = "deltalake-rb"
  spec.version       = DeltaLake::VERSION
  spec.summary       = "Delta Lake for Ruby"
  spec.homepage      = "https://github.com/ankane/delta-ruby"
  spec.license       = "Apache-2.0"

  spec.author        = "Andrew Kane"
  spec.email         = "andrew@ankane.org"

  spec.files         = Dir["*.{md,txt}", "{ext,lib}/**/*", "Cargo.*"]
  spec.require_path  = "lib"
  spec.extensions    = ["ext/deltalake/extconf.rb"]

  spec.required_ruby_version = ">= 3.3"

  spec.add_dependency "rb_sys"
end
