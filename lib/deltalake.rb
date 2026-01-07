# ext
begin
  require "deltalake/#{RUBY_VERSION.to_f}/deltalake"
rescue LoadError
  require "deltalake/deltalake"
end

# stdlib
require "json"
require "time"

# modules
require_relative "deltalake/field"
require_relative "deltalake/metadata"
require_relative "deltalake/schema"
require_relative "deltalake/table"
require_relative "deltalake/table_alterer"
require_relative "deltalake/table_merger"
require_relative "deltalake/table_optimizer"
require_relative "deltalake/utils"
require_relative "deltalake/version"

module DeltaLake
  class Error < StandardError; end
  class TableNotFoundError < Error; end
  class DeltaProtocolError < Error; end
  class CommitFailedError < Error; end
  class SchemaMismatchError < Error; end

  class Todo < Error
    def message
      "not implemented yet"
    end
  end

  ProtocolVersions =
    Struct.new(
      :min_reader_version,
      :min_writer_version,
      :writer_features,
      :reader_features
    )

  CommitProperties =
    Struct.new(
      :custom_metadata,
      :max_commit_retries,
      # TODO
      # :app_transactions,
      keyword_init: true
    )

  PostCommitHookProperties =
    Struct.new(
      :create_checkpoint,
      :cleanup_expired_logs,
      keyword_init: true
    )

  class ArrowArrayStream
    def arrow_c_stream
      self
    end
  end

  class << self
    def write(
      table_or_uri,
      data,
      partition_by: nil,
      mode: "error",
      name: nil,
      description: nil,
      configuration: nil,
      schema_mode: nil,
      storage_options: nil,
      predicate: nil,
      target_file_size: nil,
      writer_properties: nil,
      commit_properties: nil,
      post_commithook_properties: nil
    )
      table, table_uri = try_get_table_and_table_uri(table_or_uri, storage_options)

      if partition_by.is_a?(String)
        partition_by = [partition_by]
      end

      if !table.nil? && mode == "ignore"
        return
      end

      data = Utils.convert_data(data)

      if table
        table._table.write(
          data,
          mode,
          schema_mode,
          partition_by,
          predicate,
          target_file_size,
          name,
          description,
          configuration,
          writer_properties,
          commit_properties,
          post_commithook_properties
        )
      else
        write_deltalake_rust(
          table_uri,
          data,
          mode,
          schema_mode,
          partition_by,
          predicate,
          target_file_size,
          name,
          description,
          configuration,
          storage_options,
          writer_properties,
          commit_properties,
          post_commithook_properties
        )
      end
    end

    private

    def try_get_table_and_table_uri(table_or_uri, storage_options)
      if !table_or_uri.is_a?(String) && !table_or_uri.is_a?(Table)
        raise ArgumentError, "table_or_uri must be a String or Table"
      end

      if table_or_uri.is_a?(String)
        table = try_get_deltatable(table_or_uri, storage_options)
        table_uri = table_or_uri.to_s
      else
        table = table_or_uri
        table_uri = table._table.table_uri
      end

      [table, table_uri]
    end

    def try_get_deltatable(table_uri, storage_options)
      Table.new(table_uri, storage_options: storage_options)
    rescue TableNotFoundError
      nil
    end
  end
end
