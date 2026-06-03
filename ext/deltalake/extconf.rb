require "mkmf"
require "rb_sys/mkmf"

create_rust_makefile("deltalake/deltalake") do |r|
  features = []
  features << "s3" if enable_config("s3") || ENV["DELTALAKE_S3"]
  features << "azure" if enable_config("azure") || ENV["DELTALAKE_AZURE"]
  features << "gcs" if enable_config("gcs") || ENV["DELTALAKE_GCS"]
  r.features = features
end
