require_relative "test_helper"

class AlterTest < Minitest::Test
  def test_add_feature
    df = Polars::DataFrame.new({"a" => [1, 2, 3]})
    with_table(df) do |dt|
      error = assert_raises(DeltaLake::Error) do
        dt.alter.add_feature(:append_only)
      end
      assert_match "Table feature enables writer feature, but min_writer is not v7", error.message

      dt.alter.add_feature(:append_only, allow_protocol_versions_increase: true)

      protocol = dt.protocol
      assert_equal 1, protocol.min_reader_version
      assert_equal 7, protocol.min_writer_version
      assert_equal ["appendOnly"], protocol.writer_features
      assert_nil protocol.reader_features

      error = assert_raises(ArgumentError) do
        dt.alter.add_feature(:missing)
      end
      assert_equal "Invalid feature", error.message
    end
  end

  def test_add_columns
    df = Polars::DataFrame.new({"a" => [1, 2, 3]})
    with_table(df) do |dt|
      # TODO improve
      dt.alter.add_columns([])

      assert_equal 1, dt.schema.fields.size
    end
  end

  def test_add_constraint
    df = Polars::DataFrame.new({"a" => [1, 2, 3]})
    with_table(df) do |dt|
      error = assert_raises(DeltaLake::DeltaProtocolError) do
        dt.alter.add_constraint({"a_gt_1" => "a > 1"})
      end
      assert_match "Check or Invariant (a > 1) violated", error.message

      dt.alter.add_constraint({"a_gt_0" => "a > 0"})

      df2 = Polars::DataFrame.new({"a" => [4, 5, -1]})
      error = assert_raises(DeltaLake::DeltaProtocolError) do
        DeltaLake.write(dt, df2, mode: "append")
      end
      assert_match "Check or Invariant (a > 0) violated", error.message
    end
  end

  def test_drop_constraint
    df = Polars::DataFrame.new({"a" => [1, 2, 3]})
    with_table(df) do |dt|
      dt.alter.add_constraint({"a_gt_0" => "a > 0"})

      dt.alter.drop_constraint("a_gt_0")

      df2 = Polars::DataFrame.new({"a" => [4, 5, -1]})
      DeltaLake.write(dt, df2, mode: "append")
    end
  end

  def test_drop_constraint_missing
    df = Polars::DataFrame.new({"a" => [1, 2, 3]})
    with_table(df) do |dt|
      error = assert_raises(DeltaLake::Error) do
        dt.alter.drop_constraint("a_gt_0")
      end
      assert_equal "Generic DeltaTable error: Constraint with name: a_gt_0 doesn't exists", error.message

      dt.alter.drop_constraint("a_gt_0", raise_if_not_exists: false)
    end
  end

  def test_set_table_properties
    df = Polars::DataFrame.new({"a" => [1, 2, 3]})
    with_table(df) do |dt|
      dt.alter.set_table_properties({"delta.enableChangeDataFeed" => "true"})

      error = assert_raises(DeltaLake::Error) do
        dt.alter.set_table_properties({"missing" => "true"})
      end
      assert_equal "Kernel: Generic delta kernel error: Error parsing property 'missing':'true'", error.message
    end
  end
end
