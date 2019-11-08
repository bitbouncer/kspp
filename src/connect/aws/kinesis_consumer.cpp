#include <kspp/connect/aws/kinesis_consumer.h>
#include <kspp/features/aws/aws.h>

#include <aws/core/utils/Outcome.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/kinesis/model/DescribeStreamRequest.h>
#include <aws/kinesis/model/DescribeStreamResult.h>
#include <aws/kinesis/model/GetRecordsRequest.h>
#include <aws/kinesis/model/GetRecordsResult.h>
#include <aws/kinesis/model/GetShardIteratorRequest.h>
#include <aws/kinesis/model/GetShardIteratorResult.h>
#include <aws/kinesis/model/Shard.h>

using namespace std::chrono_literals;

// inspiration here
//https://github.com/pipelinedb/pipeline_kinesis/blob/master/kinesis_consumer.cpp

namespace kspp {
  kinesis_consumer::kinesis_consumer(int32_t partition, std::string stream_name)
  : partition_(partition)
  , stream_name_(stream_name)
    , commit_chain_(stream_name, partition)
    , bg_([this] { _thread(); }){
    kspp::init_aws(); // needs to be done once
    //offset_storage_ = get_offset_provider(tp.offset_storage);
  }

  kinesis_consumer::~kinesis_consumer() {
    close();
    bg_.join();
    commit(true);
    client_.reset(nullptr);
    LOG(INFO) << "kinesis_consumer table:" << stream_name_ << ":" << partition_ << ", closed - consumed " << msg_cnt_ << " messages";
  }

  void kinesis_consumer::close() {
    if (closed_)
      return;
    closed_ = true;
    exit_ = true;
    start_running_ = false;
  }

  bool kinesis_consumer::initialize() {
    // todo move this to a generic aws connection properties since this seems to be generic...
    std::string aws_access_key_id;
    std::string aws_secret_access_key;
    std::string aws_session_token;
    std::string aws_region;
    std::string aws_custom_endpoint;
    {
      const char *s = std::getenv("AWS_ACCESS_KEY_ID");
      if (s) {
        aws_access_key_id = s;
      }
    }

    {
      const char *s = std::getenv("AWS_SECRET_ACCESS_KEY");
      if (s) {
        aws_secret_access_key = s;
      }
    }

    {
      const char *s = std::getenv("AWS_SESSION_TOKEN");
      if (s) {
        aws_session_token = s;
      }
    }

    {
      const char *s = std::getenv("AWS_REGION");
      if (s) {
        aws_region = s;
      }
    }

    {
      const char *s = std::getenv("AWS_CUSTOM_ENDPOINT");
      if (s) {
        aws_custom_endpoint = s;
      }
    }


    LOG(INFO) << "AWS_ACCESS_KEY_ID:       " << aws_access_key_id;
    LOG(INFO) << "AWS_SECRET_ACCESS_KEY:   " << aws_secret_access_key; // todo remove this log
    LOG(INFO) << "AWS_SESSION_TOKEN:       " << aws_session_token;
    LOG(INFO) << "AWS_REGION:              " << aws_region;
    LOG(INFO) << "AWS_CUSTOM_ENDPOINT:     " << aws_custom_endpoint;

    if (aws_access_key_id.size() && aws_secret_access_key.size() && aws_session_token.size()){
      // this is probably debugging with copied credentials and the full iam token TODO fix the endpoint
      Aws::Auth::AWSCredentials credentials(Aws::String(aws_access_key_id.c_str()), Aws::String(aws_secret_access_key.c_str()), Aws::String(aws_session_token.c_str()));
      Aws::Client::ClientConfiguration clientConfig;
      clientConfig.endpointOverride = aws_custom_endpoint;
      clientConfig.connectTimeoutMs = 5000;
      clientConfig.requestTimeoutMs = 1000;
      clientConfig.region = aws_region;
      client_ = std::make_unique<Aws::Kinesis::KinesisClient>(credentials, clientConfig);
    } else if (aws_access_key_id.size() && aws_secret_access_key.size()){
      Aws::Auth::AWSCredentials credentials(Aws::String(aws_access_key_id.c_str()), Aws::String(aws_secret_access_key.c_str()));
      Aws::Client::ClientConfiguration clientConfig;
      clientConfig.endpointOverride = aws_custom_endpoint;
      clientConfig.connectTimeoutMs = 5000;
      clientConfig.requestTimeoutMs = 1000;
      clientConfig.region = aws_region;
      client_ = std::make_unique<Aws::Kinesis::KinesisClient>(credentials, clientConfig);
    } else {
      Aws::Client::ClientConfiguration clientConfig;
      clientConfig.connectTimeoutMs = 5000;
      clientConfig.requestTimeoutMs = 1000;
      clientConfig.region = aws_region;
      client_ = std::make_unique<Aws::Kinesis::KinesisClient>(clientConfig); // picks up default iam role from the machined or env
    }

    // Describe shards - do we have to do this???
    Aws::Kinesis::Model::DescribeStreamRequest describeStreamRequest;
    describeStreamRequest.SetStreamName(stream_name_.c_str());
    Aws::Vector<Aws::Kinesis::Model::Shard> shards;
    Aws::String exclusiveStartShardId = "";
    do
    {
      Aws::Kinesis::Model::DescribeStreamOutcome describeStreamResult = client_->DescribeStream(describeStreamRequest);
      Aws::Vector<Aws::Kinesis::Model::Shard> shardsTemp = describeStreamResult.GetResult().GetStreamDescription().GetShards();
      shards.insert(shards.end(), shardsTemp.begin(), shardsTemp.end());
      LOG(INFO) << describeStreamResult.GetError().GetMessage();
      if (describeStreamResult.GetResult().GetStreamDescription().GetHasMoreShards() && shards.size() > 0)
      {
        exclusiveStartShardId = shards[shards.size() - 1].GetShardId();
        describeStreamRequest.SetExclusiveStartShardId(exclusiveStartShardId);
      }
      else
        exclusiveStartShardId = "";
    } while (exclusiveStartShardId.length() != 0);

    Aws::Kinesis::Model::GetShardIteratorRequest getShardIteratorRequest;
    getShardIteratorRequest.SetStreamName(stream_name_.c_str());
    // use our partition TODO check if this order is constant ???
    getShardIteratorRequest.SetShardId(shards[partition_].GetShardId());
    //getShardIteratorRequest.SetShardIteratorType(Aws::Kinesis::Model::ShardIteratorType::TRIM_HORIZON);  // this is earliest -  fixme
    getShardIteratorRequest.SetShardIteratorType(Aws::Kinesis::Model::ShardIteratorType::LATEST);  // this is earliest -  fixme
    Aws::Kinesis::Model::GetShardIteratorOutcome getShardIteratorResult = client_->GetShardIterator(getShardIteratorRequest);
    // todo check result
    shard_iterator_ = getShardIteratorResult.GetResult().GetShardIterator();
    start_running_ = true;
    return true;
  }

  void kinesis_consumer::start(int64_t offset) {
    /*int64_t tmp = offset_storage_->start(offset);
    read_cursor_.start(tmp);
    if (tmp>0)
      read_cursor_.set_eof(true); // use rescrape for the first item ie enabled
    */
    initialize();
  }

  void kinesis_consumer::_thread() {
    while (!exit_) {
      // connected
      if (!start_running_) {
        std::this_thread::sleep_for(1s);
        continue;
      }

      if (_incomming_msg.size()>100000) {
        std::this_thread::sleep_for(100ms);
        continue;
      }

      Aws::Kinesis::Model::GetRecordsRequest getRecordsRequest;
      getRecordsRequest.SetShardIterator(shard_iterator_);
      auto t0 = kspp::milliseconds_since_epoch();
      Aws::Kinesis::Model::GetRecordsOutcome getRecordsResult = client_->GetRecords(getRecordsRequest);
      auto t1 = kspp::milliseconds_since_epoch();
      auto& v = getRecordsResult.GetResult().GetRecords();
      LOG_EVERY_N(INFO, 100) << "Kinesis::GetRecords, got: " << v.size() << " records - in " << t1-t0 << " milliseconds";
      eof_ = (v.size()==0); // is this true in middle of stream??
      for (auto r : v) {
        int64_t ts = r.GetApproximateArrivalTimestamp().Millis();
        auto value = std::make_shared<std::string>((char*)r.GetData().GetUnderlyingData(), r.GetData().GetLength());
        const std::string key(r.GetPartitionKey());
        auto record = std::make_shared<krecord<std::string, std::string>>(key, value, ts);
        auto e = std::make_shared<kevent<std::string, std::string>>(record, nullptr); // no commit chain for now
        _incomming_msg.push_back(e);
        // do we have one...
        //int64_t tick = read_cursor_.last_tick();
        //auto e = std::make_shared<kevent<kspp::generic_avro, kspp::generic_avro>>(record, tick  > 0 ? commit_chain_.create(tick) : nullptr);
        //assert(e.get()!=nullptr);
        msg_cnt_++;
      }
      int64_t sleep_time = 250 -(t1 - t0);
      if (sleep_time>0 && sleep_time<250)
        std::this_thread::sleep_for(std::chrono::milliseconds(sleep_time));
      shard_iterator_ = getRecordsResult.GetResult().GetNextShardIterator();
    }
    DLOG(INFO) << "exiting thread";
  }
}