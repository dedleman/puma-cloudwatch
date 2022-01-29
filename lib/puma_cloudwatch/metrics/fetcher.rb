require "json"
require "uri"
require "socket"

class PumaCloudwatch::Metrics
  class Fetcher
    def initialize(options={})
      @control_url = options[:control_url]
      @control_auth_token = options[:control_auth_token]
    end

    def call
      body = with_retries do
        read_socket
      end
      JSON.parse(body.split("\n").last) # stats
    end

  private
    def read_socket
      uri = URI.parse @control_url
      http_get_string = "GET /stats?token=#{@control_auth_token} HTTP/1.0\r\n\r\n"

      socket = case uri.scheme
      when 'tcp'
        Socket.tcp(uri.host, uri.port)
      when 'unix'
        Socket.unix("#{uri.host}#{uri.path}")
      else
        raise "Invalid scheme: #{uri.scheme}"
      end

      socket.print(http_get_string)
      socket.read
    ensure
      socket.close if socket && !socket.closed?
    end

    def with_retries
      retries, max_attempts = 0, 10
      yield
    rescue Errno::ENOENT => e
      retries += 1
      if retries > max_attempts
        raise e
      end
      puts "retries #{retries} #{e.class} #{e.message}" if ENV['PUMA_CLOUDWATCH_SOCKET_RETRY']
      sleep 1
      retry
    end
  end
end
