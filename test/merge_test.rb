require_relative "test_helper"

class MergeTest < Minitest::Test
  def test_when_matched_update
    df = Polars::DataFrame.new({"x" => [1, 2, 3], "y" => [4, 5, 6]})
    with_table(df) do |dt|
      source = Polars::DataFrame.new({"x" => [2, 3], "y" => [5, 8]})

      metrics =
        dt.merge(source, "target.x = source.x", source_alias: "source", target_alias: "target")
          .when_matched_update({"x" => "source.x", "y" => "source.y"})
          .execute
      assert_equal 3, metrics[:num_output_rows]
      assert_equal 1, metrics[:num_target_files_added]
      assert_equal 1, metrics[:num_target_files_removed]

      expected = Polars::DataFrame.new({"x" => [1, 2, 3], "y" => [4, 5, 8]})
      assert_frame_equal expected, dt.to_polars.sort("x")
    end
  end

  def test_when_not_matched_insert
    df = Polars::DataFrame.new({"x" => [1, 2, 3], "y" => [4, 5, 6]})
    with_table(df) do |dt|
      source = Polars::DataFrame.new({"x" => [2, 3, 7], "y" => [4, 5, 8]})

      metrics =
        dt.merge(source, "target.x = source.x", source_alias: "source", target_alias: "target")
          .when_not_matched_insert({"x" => "source.x", "y" => "source.y"})
          .execute
      assert_equal 1, metrics[:num_output_rows]
      assert_equal 1, metrics[:num_target_files_added]
      assert_equal 0, metrics[:num_target_files_removed]

      expected = Polars::DataFrame.new({"x" => [1, 2, 3, 7], "y" => [4, 5, 6, 8]})
      assert_frame_equal expected, dt.to_polars.sort("x")
    end
  end

  def test_when_matched_delete
    df = Polars::DataFrame.new({"x" => [1, 2, 3], "y" => [4, 5, 6]})
    with_table(df) do |dt|
      source = Polars::DataFrame.new({"x" => [2, 3], "deleted" => [false, true]})

      metrics =
        dt.merge(source, "target.x = source.x", source_alias: "source", target_alias: "target")
          .when_matched_delete(predicate: "source.deleted = true")
          .execute
      assert_equal 2, metrics[:num_output_rows]
      assert_equal 1, metrics[:num_target_files_added]
      assert_equal 1, metrics[:num_target_files_removed]

      expected = Polars::DataFrame.new({"x" => [1, 2], "y" => [4, 5]})
      assert_frame_equal expected, dt.to_polars.sort("x")
    end
  end

  def test_multiple
    df = Polars::DataFrame.new({"x" => [1, 2, 3], "y" => [4, 5, 6]})
    with_table(df) do |dt|
      source = Polars::DataFrame.new({"x" => [2, 3, 5], "y" => [5, 8, 11]})

      metrics =
        dt.merge(source, "target.x = source.x", source_alias: "source", target_alias: "target")
          .when_matched_delete(predicate: "source.x = target.x")
          .when_matched_update({"x" => "source.x", "y" => "source.y"})
          .execute
      assert_equal 1, metrics[:num_output_rows]
      assert_equal 1, metrics[:num_target_files_added]
      assert_equal 1, metrics[:num_target_files_removed]

      expected = Polars::DataFrame.new({"x" => [1], "y" => [4]})
      assert_frame_equal expected, dt.to_polars
    end
  end

  def test_when_not_matched_by_source_update
    df = Polars::DataFrame.new({"x" => [1, 2, 3], "y" => [4, 5, 6]})
    with_table(df) do |dt|
      source = Polars::DataFrame.new({"x" => [2, 3, 4]})

      metrics =
        dt.merge(source, "target.x = source.x", source_alias: "source", target_alias: "target")
          .when_not_matched_by_source_update({"`y`" => "0"}, predicate: "`y` > 3")
          .execute
      assert_equal 3, metrics[:num_output_rows]
      assert_equal 1, metrics[:num_target_files_added]
      assert_equal 1, metrics[:num_target_files_removed]

      expected = Polars::DataFrame.new({"x" => [1, 2, 3], "y" => [0, 5, 6]})
      assert_frame_equal expected, dt.to_polars.sort("x")
    end
  end
end
