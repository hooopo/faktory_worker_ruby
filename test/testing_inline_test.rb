require 'helper'

class TestingInlineTest < LiveTest
  describe 'faktory inline testing' do
    class InlineError < RuntimeError; end
    class ParameterIsNotString < RuntimeError; end

    class InlineJob
      include Faktory::Job
      def perform(pass)
        raise InlineError unless pass
      end
    end

    class InlineJobWithTimeParam
      include Faktory::Job
      def perform(time)
        raise ParameterIsNotString unless time.is_a?(String) || time.is_a?(Numeric)
      end
    end

    before do
      require 'faktory/testing'
    end

    after do
      Faktory::Testing.disable!
    end

    it 'stubs the async call when in testing mode' do
      Faktory::Testing.inline! do
        assert InlineJob.perform_async(true)

        assert_raises InlineError do
          InlineJob.perform_async(false)
        end
      end
    end

    it 'stubs the push call when in testing mode' do
      Faktory::Testing.inline! do
        client = Faktory::Client.new
        assert client.push({
          "jid" => SecureRandom.uuid,
          "queue" => "default",
          "jobtype" => InlineJob,
          "args" => [true]
        })

        assert_raises InlineError do
          client.push({
            "jid" => SecureRandom.uuid,
            "queue" => "default",
            "jobtype" => InlineJob,
            "args" => [false]
          })
        end
      end
    end
  end
end
