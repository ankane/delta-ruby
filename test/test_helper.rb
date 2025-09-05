require "bundler/setup"
Bundler.require(:default)
require "minitest/autorun"

class Minitest::Test
  include Polars::Testing

  def setup
    GC.stress = true if stress?
  end

  def teardown
    GC.stress = false if stress?
  end

  def stress?
    ENV["STRESS"]
  end

  def with_new_table
    prefix = ENV["CLOUD_PREFIX"]

    if prefix
      if prefix.start_with?("s3://")
        ENV["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true"
      end
      yield "#{prefix}/delta-ruby-test/#{Time.now.to_f}"
    else
      Dir.mktmpdir do |table_uri|
        yield table_uri
      end
    end
  end

  def with_table(df, **write_options)
    with_new_table do |table_uri|
      DeltaLake.write(table_uri, df, **write_options)
      yield DeltaLake::Table.new(table_uri)
    end
  end
end
