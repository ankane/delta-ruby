require_relative "test_helper"

class OptimizeTest < Minitest::Test
  def test_compact
    df = Polars::DataFrame.new({"a" => [1, 2, 3]})
    with_table(df) do |dt|
      2.times do
        DeltaLake.write(dt, df, mode: "append")
      end

      metrics = dt.optimize.compact
      assert_equal 1, metrics["numFilesAdded"]
      assert_equal 3, metrics["numFilesRemoved"]
      assert_in_delta 544, metrics["filesAdded"]["avg"]
      assert_equal 544, metrics["filesAdded"]["max"]
      assert_equal 544, metrics["filesAdded"]["min"]
      assert_equal 1, metrics["filesAdded"]["totalFiles"]
      assert_equal 544, metrics["filesAdded"]["totalSize"]
      assert_in_delta 530, metrics["filesRemoved"]["avg"]
      assert_equal 530, metrics["filesRemoved"]["max"]
      assert_equal 530, metrics["filesRemoved"]["min"]
      assert_equal 3, metrics["filesRemoved"]["totalFiles"]
      assert_equal 1590, metrics["filesRemoved"]["totalSize"]
      assert_equal 1, metrics["partitionsOptimized"]
      assert_equal 3, metrics["numBatches"]
      assert_equal 3, metrics["totalConsideredFiles"]
      assert_equal 0, metrics["totalFilesSkipped"]
      assert_equal true, metrics["preserveInsertionOrder"]
    end
  end

  def test_z_order
    df = Polars::DataFrame.new({"a" => [1, 2, 3]})
    with_table(df) do |dt|
      2.times do
        DeltaLake.write(dt, df, mode: "append")
      end

      metrics = dt.optimize.z_order(["a"])
      assert_equal 1, metrics["numFilesAdded"]
      assert_equal 3, metrics["numFilesRemoved"]
      assert_in_delta 544, metrics["filesAdded"]["avg"]
      assert_equal 544, metrics["filesAdded"]["max"]
      assert_equal 544, metrics["filesAdded"]["min"]
      assert_equal 1, metrics["filesAdded"]["totalFiles"]
      assert_equal 544, metrics["filesAdded"]["totalSize"]
      assert_in_delta 530, metrics["filesRemoved"]["avg"]
      assert_equal 530, metrics["filesRemoved"]["max"]
      assert_equal 530, metrics["filesRemoved"]["min"]
      assert_equal 3, metrics["filesRemoved"]["totalFiles"]
      assert_equal 1590, metrics["filesRemoved"]["totalSize"]
      assert_equal 0, metrics["partitionsOptimized"]
      assert_equal 1, metrics["numBatches"]
      assert_equal 3, metrics["totalConsideredFiles"]
      assert_equal 0, metrics["totalFilesSkipped"]
      assert_equal true, metrics["preserveInsertionOrder"]
    end
  end
end
