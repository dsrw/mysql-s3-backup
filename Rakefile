DAYS_BETWEEN_FULL_BACKUPS = 14

require "backup_utils"

desc "Performs a full backup"
task :full_backup => :connect_to_s3 do
  puts "Performing full backup"
  rm Dir["*.{sql,delta,bz}"]
  file = "full-#{Time.now.utc}.sql".gsub(" ", "_")
  dump_db(file)
  `bzip2 -k #{file}`
  upload(file + ".bz2")
  rm file + ".bz2"
end

desc "Performs a delta backup"
task :delta_backup => :connect_to_s3 do
  puts "Performing delta backup"
  dump_file = "delta-#{Time.now.utc}.sql".gsub(" ", "_")
  delta_file = dump_file + ".delta"
  dump_db(dump_file)
  sh "xdelta3 -es #{full_file} #{dump_file} #{delta_file}"
  upload(delta_file)
  rm delta_file
end

desc "Performs a full or delta backup"
task :backup do
  if full_file && parse_time(full_file) > config("days_between_full_backups").days.ago && verify_upload(full_file)
    Rake::Task["delta_backup"].invoke
  else
    Rake::Task["full_backup"].invoke
  end
end

desc "Restores a backup to a .sql file"
task :restore => :connect_to_s3 do
  date = ENV["DATE"] ? Time.parse(ENV["DATE"]) : Time.now
  backup_bucket = AWS::S3::Bucket.find(bucket)
  keys = backup_bucket.map(&:key) \
                      .select {|k| k =~ /^(full|delta)/} \
                      .select {|k| parse_time(k) < date} \
                      .sort_by {|k| parse_time(k)}
  full_file = keys.find {|k| k =~ /^full/}
  delta_file = keys.find {|k| k =~ /^delta/}
  raise "Nothing to restore" unless full_file
  mkdir_p "restore"
  cd "restore" do
    download(full_file)
    puts "Uncompressing #{full_file}"
    sh "bunzip2 #{full_file}"
    full_file.sub!(".bz2", "")
    if delta_file && parse_time(delta_file) > parse_time(full_file)
      download(delta_file)
      @restore_file = delta_file.sub("delta", "restore").gsub(".delta", "")
      puts "Applying delta..."
      sh "xdelta3 -ds #{full_file} #{delta_file} #{@restore_file}"
    else
      @restore_file = full_file.sub("full", "restore")
      mv full_file, @restore_file 
    end
    cp @restore_file, ".."
  end
  rm_r "restore"
  puts "Restored #{@restore_file}"  
end

task :restore_to_database => :restore do
  puts "Writing to database..."
  load_db(@restore_file)
end

task :connect_to_s3 do
  connect_to_s3
end




