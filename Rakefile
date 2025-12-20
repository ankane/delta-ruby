require "bundler/gem_tasks"
require "rake/testtask"
require "rake/extensiontask"

Rake::TestTask.new do |t|
  t.pattern = "test/**/*_test.rb"
end

task default: :test

platforms = [
  "x86_64-linux",
  "x86_64-linux-musl",
  "aarch64-linux",
  "aarch64-linux-musl",
  "x86_64-darwin",
  "arm64-darwin",
  "x64-mingw-ucrt"
]

gemspec = Bundler.load_gemspec("deltalake-rb.gemspec")
Rake::ExtensionTask.new("deltalake", gemspec) do |ext|
  ext.lib_dir = "lib/deltalake"
  ext.cross_compile = true
  ext.cross_platform = platforms
  ext.cross_compiling do |spec|
    spec.dependencies.reject! { |dep| dep.name == "rb_sys" }
    spec.files.reject! { |file| File.fnmatch?("ext/*", file, File::FNM_EXTGLOB) }
  end
end

task :remove_ext do
  Dir["lib/deltalake/deltalake.{bundle,so}"].each do |path|
    File.unlink(path)
  end
end

Rake::Task["build"].enhance [:remove_ext]
