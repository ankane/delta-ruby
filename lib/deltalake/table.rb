module DeltaLake
  class Table
    FSCK_METRICS_FILES_REMOVED_LABEL = "files_removed"

    def initialize(
      table_uri,
      version: nil,
      storage_options: nil,
      without_files: false,
      log_buffer_size: nil
    )
      @storage_options = storage_options
      @table =
        RawDeltaTable.new(
          table_uri,
          version,
          storage_options,
          without_files,
          log_buffer_size
        )
    end

    def self.exists?(table_uri, storage_options: nil)
      RawDeltaTable.is_deltatable(table_uri, storage_options)
    end

    def version
      @table.version
    end

    def partitions
      partitions = []
      @table.get_active_partitions.each do |partition|
        next unless partition
        partitions << partition.to_h
      end
      partitions
    end

    def files(partition_filters: nil)
      @table.files(_stringify_partition_values(partition_filters))
    end

    def file_uris(partition_filters: nil)
      @table.file_uris(_stringify_partition_values(partition_filters))
    end

    def load_as_version(version)
      if version.is_a?(Integer)
        @table.load_version(version)
      elsif version.is_a?(Time)
        @table.load_with_datetime(version.utc.iso8601(9))
      elsif version.is_a?(String)
        @table.load_with_datetime(version)
      else
        raise TypeError, "Invalid datatype provided for version, only Integer, String, and Time are accepted."
      end
    end

    def load_cdf(
      starting_version: 0,
      ending_version: nil,
      starting_timestamp: nil,
      ending_timestamp: nil,
      columns: nil
    )
      @table.load_cdf(
        starting_version,
        ending_version,
        starting_timestamp,
        ending_timestamp,
        columns
      )
    end

    def table_uri
      @table.table_uri
    end

    def schema
      @table.schema
    end

    def metadata
      Metadata.new(@table)
    end

    def protocol
      ProtocolVersions.new(*@table.protocol_versions)
    end

    def history(limit: nil)
      backwards_enumerate = lambda do |iterable, start_end, &block|
        n = start_end
        iterable.each do |elem|
          block.call(n, elem)
          n -= 1
        end
      end

      commits = @table.history(limit)
      history = []
      backwards_enumerate.(commits, @table.get_latest_version) do |version, commit_info_raw|
        commit = JSON.parse(commit_info_raw)
        commit["version"] = version
        history << commit
      end
      history
    end

    def vacuum(
      retention_hours: nil,
      dry_run: true,
      enforce_retention_duration: true,
      post_commithook_properties: nil,
      commit_properties: nil
    )
      if retention_hours
        if retention_hours < 0
          raise ArgumentError, "The retention periods should be positive."
        end
      end

      @table.vacuum(
        dry_run,
        retention_hours,
        enforce_retention_duration,
        commit_properties,
        post_commithook_properties
      )
    end

    def optimize
      TableOptimizer.new(self)
    end

    def alter
      TableAlterer.new(self)
    end

    def merge(
      source,
      predicate,
      source_alias: nil,
      target_alias: nil,
      error_on_type_mismatch: true,
      writer_properties: nil,
      post_commithook_properties: nil,
      commit_properties: nil
    )
      source = Utils.convert_data(source)

      rb_merge_builder =
        @table.create_merge_builder(
          source,
          predicate,
          source_alias,
          target_alias,
          !error_on_type_mismatch,
          writer_properties,
          post_commithook_properties,
          commit_properties
        )
      TableMerger.new(rb_merge_builder, @table)
    end

    def restore(
      target,
      ignore_missing_files: false,
      protocol_downgrade_allowed: false,
      commit_properties: nil
    )
      if target.is_a?(Time)
        metrics =
          @table.restore(
            target.utc.iso8601(9),
            ignore_missing_files,
            protocol_downgrade_allowed,
            commit_properties
          )
      else
        metrics =
          @table.restore(
            target,
            ignore_missing_files,
            protocol_downgrade_allowed,
            commit_properties
          )
      end
      JSON.parse(metrics)
    end

    def to_polars(eager: true, rechunk: false, columns: nil)
      require "polars-df"

      sources = file_uris
      if sources.empty?
        # TODO pass schema
        lf = Polars::LazyFrame.new
      else
        delta_keys = [
          "AWS_S3_ALLOW_UNSAFE_RENAME",
          "AWS_S3_LOCKING_PROVIDER",
          "CONDITIONAL_PUT",
          "DELTA_DYNAMO_TABLE_NAME"
        ]
        storage_options = @storage_options&.reject { |k, _| delta_keys.include?(k.to_s.upcase) }

        scan_options =
          if Gem::Version.new(Polars::VERSION) >= Gem::Version.new("0.24")
            {missing_columns: "insert"}
          else
            {allow_missing_columns: true}
          end

        lf =
          Polars.scan_parquet(
            sources,
            hive_partitioning: true,
            storage_options: storage_options,
            rechunk: rechunk,
            **scan_options
          )

        if columns
          # by_name requires polars-df > 0.15.0
          lf = lf.select(Polars.cs.by_name(*columns))
        end
      end

      eager ? lf.collect : lf
    end

    def update_incremental
      @table.update_incremental
    end

    def delete(
      predicate = nil,
      writer_properties: nil,
      post_commithook_properties: nil,
      commit_properties: nil
    )
      metrics =
        @table.delete(
          predicate,
          writer_properties,
          post_commithook_properties,
          commit_properties
        )
      JSON.parse(metrics).transform_keys(&:to_sym)
    end

    def repair(
      dry_run: false,
      post_commithook_properties: nil,
      commit_properties: nil
    )
      metrics =
        @table.repair(
          dry_run,
          commit_properties,
          post_commithook_properties
        )
      deserialized_metrics = JSON.parse(metrics)
      deserialized_metrics[FSCK_METRICS_FILES_REMOVED_LABEL] = JSON.parse(
        deserialized_metrics[FSCK_METRICS_FILES_REMOVED_LABEL]
      )
      deserialized_metrics.transform_keys(&:to_sym)
    end

    def transaction_version(app_id)
      @table.transaction_version(app_id)
    end

    # private
    def _table
      @table
    end

    # private
    def _stringify_partition_values(partition_filters)
      if partition_filters.nil?
        return partition_filters
      end

      raise Todo
    end
  end
end
