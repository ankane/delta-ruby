require_relative "test_helper"

class TableTest < Minitest::Test
  def test_to_polars
    df = Polars::DataFrame.new({"a" => [1, 2, 3]})
    with_table(df) do |dt|
      assert_frame_equal df, dt.to_polars
      assert_frame_equal df, dt.to_polars(eager: false).collect
    end
  end

  def test_files
    df = Polars::DataFrame.new({"a" => [1, 2, 3]})
    with_table(df) do |dt|
      assert_equal 1, dt.file_uris.length
      assert_equal 1, dt.files.length
    end
  end

  def test_partition_by
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => [4, 4, 5]})
    with_table(df, partition_by: "b") do |dt|
      assert_frame_equal df, dt.to_polars.sort("a")
    end
  end

  def test_metadata
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => [4, 4, 5]})
    with_table(df, name: "hello", description: "world", partition_by: "b") do |dt|
      metadata = dt.metadata
      assert_kind_of String, metadata.id
      assert_equal "hello", metadata.name
      assert_equal "world", metadata.description
      assert_equal ["b"], metadata.partition_columns
      # consistent with Python
      assert_kind_of Integer, metadata.created_time
      assert_empty metadata.configuration

      partitions = dt.partitions
      assert_equal 2, partitions.size
      assert_includes partitions, {"b" => "4"}
      assert_includes partitions, {"b" => "5"}
    end
  end

  def test_protocol
    df = Polars::DataFrame.new({"a" => [1, 2, 3]})
    with_table(df) do |dt|
      protocol = dt.protocol
      assert_equal 1, protocol.min_reader_version
      assert_equal 2, protocol.min_writer_version
      assert_nil protocol.reader_features
      assert_nil protocol.writer_features
    end
  end

  def test_schema
    df = Polars::DataFrame.new({"a" => [1, 2, 3]})
    with_table(df) do |dt|
      schema = dt.schema
      assert_equal 1, schema.fields.length
      assert_equal "a", schema.fields[0].name
      assert_equal "long", schema.fields[0].type
      assert_equal true, schema.fields[0].nullable
      assert_match %!@fields=[<DeltaLake::Field name="a", type="long", nullable=true>]!, dt.schema.inspect
    end
  end

  def test_history
    df = Polars::DataFrame.new({"a" => [1, 2, 3]})
    with_table(df) do |dt|
      dt.delete("a > 1")

      history = dt.history
      assert_equal 2, history.size
      assert_equal "DELETE", history[0]["operation"]
      assert_equal 1, history[0]["version"]
      assert_equal "WRITE", history[1]["operation"]
      assert_equal 0, history[1]["version"]

      assert_equal 1, dt.history(limit: 1).size
    end
  end

  def test_load_cdf
    df = Polars::DataFrame.new({"a" => [1, 2, 3]})
    with_table(df) do |dt|
      DeltaLake.write(dt, df, mode: "overwrite")
      DeltaLake.write(dt, df, mode: "append")

      cdf = dt.load_cdf(starting_version: 1, ending_version: 2)
      assert_kind_of DeltaLake::ArrowArrayStream, cdf

      assert_equal 2, Polars::DataFrame.new(cdf).n_unique(subset: ["_commit_version"])

      error = assert_raises(ArgumentError) do
        Polars::DataFrame.new(cdf)
      end
      assert_equal "the C stream was already released", error.message

      cdf = dt.load_cdf(starting_version: 1, ending_version: 2, columns: ["a", "_commit_timestamp"])
      assert_equal ["a", "_commit_timestamp"], Polars::DataFrame.new(cdf).columns
    end
  end

  def test_delete
    df = Polars::DataFrame.new({"a" => [1, 2, 3]})
    with_table(df) do |dt|
      metrics = dt.delete("a > 1")
      assert_equal 1, metrics[:num_added_files]
      assert_equal 1, metrics[:num_removed_files]
      assert_equal 2, metrics[:num_deleted_rows]
      assert_equal 1, metrics[:num_copied_rows]

      expected = Polars::DataFrame.new({"a" => [1]})
      assert_frame_equal expected, dt.to_polars

      dt.delete
      assert_empty dt.to_polars
    end
  end

  def test_vacuum
    df = Polars::DataFrame.new({"a" => [1, 2, 3]})
    with_table(df) do |dt|
      assert_empty dt.vacuum
      assert_empty dt.vacuum(retention_hours: 0, enforce_retention_duration: false)

      dt.delete

      assert_empty dt.vacuum

      # fix flakiness
      sleep(0.001)

      assert_equal 1, dt.vacuum(retention_hours: 0, enforce_retention_duration: false).size
      assert_equal 1, dt.vacuum(dry_run: false, retention_hours: 0, enforce_retention_duration: false).size
      assert_empty dt.vacuum(retention_hours: 0, enforce_retention_duration: false)

      error = assert_raises(DeltaLake::Error) do
        dt.vacuum(retention_hours: 0)
      end
      assert_match "minimum retention for vacuum is configured to be greater than 168 hours", error.message
    end
  end

  def test_vacuum_commit_properties
    df = Polars::DataFrame.new({"a" => [1, 2, 3]})
    with_table(df) do |dt|
      dt.delete

      # fix flakiness
      sleep(0.001)

      dt.vacuum(
        dry_run: false,
        retention_hours: 0,
        enforce_retention_duration: false,
        commit_properties: DeltaLake::CommitProperties.new(custom_metadata: {"hello" => "world"})
      )

      history = dt.history(limit: 1)
      assert_equal "world", history[0]["hello"]
    end
  end

  def test_repair
    df = Polars::DataFrame.new({"a" => [1, 2, 3]})
    with_table(df) do |dt|
      metrics = dt.repair(dry_run: true)
      assert_equal true, metrics[:dry_run]
      assert_empty metrics[:files_removed]

      metrics = dt.repair
      assert_equal false, metrics[:dry_run]
      assert_empty metrics[:files_removed]
    end
  end

  def test_restore
    df = Polars::DataFrame.new({"a" => [1, 2, 3]})
    with_table(df) do |dt|
      df2 = Polars::DataFrame.new({"a" => [4, 5, 6]})
      DeltaLake.write(dt, df2, mode: "overwrite")

      metrics = dt.restore(dt.version - 1)
      assert_equal 1, metrics["numRemovedFile"]
      assert_equal 1, metrics["numRestoredFile"]

      assert_equal 2, dt.version
      assert_frame_equal df, dt.to_polars
    end
  end

  def test_missing
    with_new_table do |table_uri|
      error = assert_raises(DeltaLake::TableNotFoundError) do
        DeltaLake::Table.new(table_uri)
      end
      assert_equal "Generic delta kernel error: No files in log segment", error.message
      assert_equal false, DeltaLake::Table.exists?(table_uri)
    end
  end
end
