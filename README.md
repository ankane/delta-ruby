# delta-ruby

[Delta Lake](https://delta.io/) for Ruby

Supports local files and Amazon S3

[![Build Status](https://github.com/ankane/delta-ruby/actions/workflows/build.yml/badge.svg)](https://github.com/ankane/delta-ruby/actions)

## Installation

Add this line to your application’s Gemfile:

```ruby
gem "deltalake-rb"
```

It can take 5-10 minutes to compile the gem.

## Getting Started

Write data from [Polars](https://github.com/ankane/ruby-polars)

```ruby
df = Polars::DataFrame.new({"id" => [1, 2], "value" => [3.0, 4.0]})
DeltaLake.write("./events", df)
```

Load a table

```ruby
dt = DeltaLake::Table.new("./events")
df = dt.to_polars
```

Get a lazy frame

```ruby
lf = dt.to_polars(eager: false)
```

Append rows

```ruby
DeltaLake.write("./events", df, mode: "append")
```

Overwrite a table

```ruby
DeltaLake.write("./events", df, mode: "overwrite")
```

Add a constraint

```ruby
dt.alter.add_constraint({"id_gt_0" => "id > 0"})
```

Drop a constraint

```ruby
dt.alter.drop_constraint("id_gt_0")
```

Delete rows

```ruby
dt.delete("id > 1")
```

Vacuum

```ruby
dt.vacuum(dry_run: false)
```

Perform small file compaction

```ruby
dt.optimize.compact
```

Colocate similar data in the same files

```ruby
dt.optimize.z_order(["category"])
```

Load a previous version of a table

```ruby
dt = DeltaLake::Table.new("./events", version: 1)
# or
dt.load_as_version(1)
```

Get the schema

```ruby
dt.schema
```

Get metadata

```ruby
dt.metadata
```

Get history

```ruby
dt.history
```

## API

This library follows the [Delta Lake Python API](https://delta-io.github.io/delta-rs/) (with a few changes to make it more Ruby-like). You can follow Python tutorials and convert the code to Ruby in many cases. Feel free to open an issue if you run into problems.

## History

View the [changelog](https://github.com/ankane/delta-ruby/blob/master/CHANGELOG.md)

## Contributing

Everyone is encouraged to help improve this project. Here are a few ways you can help:

- [Report bugs](https://github.com/ankane/delta-ruby/issues)
- Fix bugs and [submit pull requests](https://github.com/ankane/delta-ruby/pulls)
- Write, clarify, or fix documentation
- Suggest or add new features

To get started with development:

```sh
git clone https://github.com/ankane/delta-ruby.git
cd delta-ruby
bundle install
bundle exec rake compile
bundle exec rake test
```
