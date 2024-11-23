require_relative "test_helper"

class WriteTest < Minitest::Test
  def test_mode
    with_new_table do |table_uri|
      df = Polars::DataFrame.new({"a" => [1, 2, 3]})
      DeltaLake.write(table_uri, df)

      dt = DeltaLake::Table.new(table_uri)
      assert_equal 0, dt.version
      assert_equal df, dt.to_polars

      error = assert_raises(DeltaLake::Error) do
        DeltaLake.write(dt, df)
      end
      assert_match "table already exists", error.message

      DeltaLake.write(dt, df, mode: "overwrite")
      assert_equal 1, dt.version
      assert_equal df, dt.to_polars

      time = Time.now
      sleep(0.01)

      DeltaLake.write(dt, df, mode: "ignore")
      assert_equal 1, dt.version
      assert_equal df, dt.to_polars

      DeltaLake.write(dt, df, mode: "append")
      assert_equal 2, dt.version
      assert_equal Polars.concat([df, df]), dt.to_polars

      assert_empty dt.transaction_versions

      cdf = dt.load_cdf(starting_version: 1, ending_version: 2)
      assert_equal 2, Polars::DataFrame.new(cdf).n_unique(subset: ["_commit_version"])

      error = assert_raises(ArgumentError) do
        Polars::DataFrame.new(cdf)
      end
      assert_equal "the C stream was already released", error.message

      dt.load_as_version(dt.version - 1)
      assert_equal 1, dt.version
      assert_equal df, dt.to_polars

      dt.load_as_version(time)
      assert_equal 1, dt.version
      assert_equal df, dt.to_polars

      dt = DeltaLake::Table.new(table_uri, version: 1)
      assert_equal 1, dt.version
      assert_equal df, dt.to_polars
    end
  end

  def test_invalid_data
    with_new_table do |table_uri|
      error = assert_raises(TypeError) do
        DeltaLake.write(table_uri, Object.new)
      end
      assert_equal "Only objects implementing the Arrow C stream interface are valid inputs for source.", error.message
    end
  end
end
