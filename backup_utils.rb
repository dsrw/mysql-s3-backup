require 'activesupport'

def connect_to_s3
  require 'aws/s3'
  AWS::S3::Base.establish_connection! :access_key_id => config("s3_key"),
                                      :secret_access_key => config("s3_secret")
end

def bucket
  @bucket ||= create_bucket
end

def create_bucket
  require 'socket'
  bucket = ENV["BUCKET"] || Socket.gethostname + "-backup"
  AWS::S3::Bucket.create(bucket)
  bucket
end

def dump_db(file)
  sh "mysqldump -u root --password=#{config("mysql_root_password")} --all-databases > #{file}"
end

def load_db(file)
  sh "mysql -u root --password=#{config("mysql_root_password")} < #{file}"
end

def full_file
  Dir["full-*.sql"].first
end

def parse_time(file)
  Time.parse file[/^\w*-(.*)\..*/,1].gsub("_"," ")
end

def upload(file) 
  puts "Uploading to S3"
  AWS::S3::S3Object.store(File.basename(file), open(file), bucket)
end

def download(file)
  puts "Downloading #{file} from s3"
  open(file, "w") {|f| f << AWS::S3::S3Object.value(file, bucket)}
end

def verify_upload(file)
  AWS::S3::Bucket.find(bucket)[file]
end

def config(key)
  require 'yaml'
  YAML.parse(`cat ~/.server_config`)[key].value
end

