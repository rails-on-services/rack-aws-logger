# frozen_string_literal: true

require "rack/aws/logger/version"

module Rack
  module Aws
    class Logger
      class Error < StandardError; end

      class << self
        attr_reader :preprocessor

        def configure(preprocessor: ->(d) { d })
          # TODO: update w/ kinesis config
          @kinesis = Aws::Kinesis::Client.new(Settings.kinesis_http_logs.to_hash.slice(*args))
          @preprocessor = preprocessor
        end

        def args
          %i[access_key_id secret_access_key region endpoint]
        end

        def logger
          @logger ||= @kinesis
        end

        def stream_name
          Settings.kinesis_http_logs[:stream_name]
        end

        def partition_key
          'partition1'
        end
      end

      def initialize(app)
        @app = app
        @executer = Concurrent::SingleThreadExecutor.new

        self.class.configure if self.class.logger.nil?
      end

      def call(env)
        start = Time.now
        response = @app.call(env)
      ensure
        log_request(env, response || $ERROR_INFO, Time.now - start)
      end

      private

      def log_request(env, response, response_time)
        @executer.post do
          record = self.class.preprocessor&.call(
            env: drop_objects(env),
            timestamp: Time.now,
            response_time: response_time,
            **format_response(response)
          )

          self.class.logger.put_record(
            stream_name: self.class.stream_name, data: record.to_json, partition_key: self.class.partition_key
          )
        rescue => exception
          Raven.capture_exception(exception)
        end
      end

      def format_response(response)
        return format_response_error(response) if response.is_a?(Exception)

        code, headers, body = response

        body = body.body if body.respond_to?(:body)
        body = [body] if body.is_a? String

        body = if headers['Content-Type']&.include? 'json'
                body.map { |s| s }
              else
                body[0..@max_body_non_json]
              end

        { code: code, body: body, headers: headers }
      end

      def format_response_error(error)
        {
          class: error.class,
          message: error.message,
          backtrace: error.backtrace
        }
      end

      def drop_objects(obj)
        allowed = [Hash, Array, String, Numeric]

        return unless allowed.reduce(false) { |m, c| m || obj.is_a?(c) }

        case obj
        when Hash
          obj.reduce({}) { |m, (k, v)| m.merge!(k => drop_objects(v)) }.compact
        when Array
          obj.map { |v| drop_objects(v) }.compact
        else
          obj
        end
      end
    end
  end
end
