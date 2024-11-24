module DeltaLake
  class TableOptimizer
    def initialize(table)
      @table = table
    end

    def compact(
      partition_filters: nil,
      target_size: nil,
      max_concurrent_tasks: nil,
      min_commit_interval: nil,
      writer_properties: nil,
      post_commithook_properties: nil,
      commit_properties: nil
    )
      metrics =
        @table._table.compact_optimize(
          partition_filters,
          target_size,
          max_concurrent_tasks,
          min_commit_interval,
          writer_properties,
          post_commithook_properties,
          commit_properties
        )
      @table.update_incremental
      result = JSON.parse(metrics)
      ["filesAdded", "filesRemoved"].each do |key|
        result[key] = JSON.parse(result[key]) if result[key].is_a?(String)
      end
      # TODO return underscore symbols like delete
      result
    end

    def z_order(
      columns,
      target_size: nil,
      max_concurrent_tasks:  nil,
      max_spill_size: 20 * 1024 * 1024 * 1024,
      min_commit_interval: nil
    )
      metrics =
        @table._table.z_order_optimize(
          Array(columns),
          target_size,
          max_concurrent_tasks,
          max_spill_size,
          min_commit_interval
        )
      @table.update_incremental
      result = JSON.parse(metrics)
      ["filesAdded", "filesRemoved"].each do |key|
        result[key] = JSON.parse(result[key]) if result[key].is_a?(String)
      end
      # TODO return underscore symbols like delete
      result
    end
  end
end
