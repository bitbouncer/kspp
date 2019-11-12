


/*
Copyright 2010-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.

This file is licensed under the Apache License, Version 2.0 (the "License").
You may not use this file except in compliance with the License. A copy of
the License is located at

http://aws.amazon.com/apache2.0/

This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
*/
#include <iostream>
#include <random>
#include <aws/core/Aws.h>
#include <aws/core/utils/Outcome.h>
#include <aws/kinesis/KinesisClient.h>
#include <aws/kinesis/model/DescribeStreamRequest.h>
#include <aws/kinesis/model/DescribeStreamResult.h>
#include <aws/kinesis/model/GetRecordsRequest.h>
#include <aws/kinesis/model/GetRecordsResult.h>
#include <aws/kinesis/model/GetShardIteratorRequest.h>
#include <aws/kinesis/model/GetShardIteratorResult.h>
#include <aws/kinesis/model/Shard.h>
#include <aws/kinesis/model/PutRecordsResult.h>
#include <aws/kinesis/model/PutRecordsRequest.h>
#include <aws/kinesis/model/PutRecordsRequestEntry.h>

#include <aws/core/auth/AWSCredentialsProvider.h>
#include <glog/logging.h>



inline int64_t milliseconds_since_epoch() {
  return std::chrono::duration_cast<std::chrono::milliseconds>
      (std::chrono::system_clock::now().time_since_epoch()).count();
}

int main(int argc, char** argv)
{
  const std::string USAGE = "\n" \
        "Usage:\n"
                            "    put_get_records <streamname>\n\n"
                            "Where:\n"
                            "    streamname - the table to delete the item from.\n\n"
                            "Example:\n"
                            "    put_get_records sample-stream\n\n";

  /*if (argc != 2)
  {
    std::cout << USAGE;
    return 1;
  }
   */


  std::string aws_access_key_id;
  std::string aws_secret_access_key;
  std::string aws_session_token;

  {
    const char *s = std::getenv("AWS_ACCESS_KEY_ID");
    if (s) {
      aws_access_key_id = s;
      LOG(INFO) << " read "  << s;
    }
  }

  {
    const char *s = std::getenv("AWS_SECRET_ACCESS_KEY");
    if (s) {
      aws_secret_access_key = s;
      LOG(INFO) << " read "  << s;
    }
  }

  {
    const char *s = std::getenv("AWS_SESSION_TOKEN");
    if (s) {
      aws_session_token = s;
      LOG(INFO) << " read "  << s;
    }
  }


  Aws::SDKOptions options;

  Aws::InitAPI(options);
  {
    const Aws::String streamName(argv[1]);

    std::random_device rd;
    std::mt19937 mt_rand(rd());

    Aws::Client::ClientConfiguration clientConfig;

    clientConfig.endpointOverride = Aws::String("https://kinesis.eu-north-1.amazonaws.com:1443");
    clientConfig.connectTimeoutMs = 5000;
    clientConfig.requestTimeoutMs = 1000;
    // set your region
    clientConfig.region = "eu-north-1";

    LOG(INFO) << "AWS_ACCESS_KEY_ID:       " << aws_access_key_id;
    LOG(INFO) << "AWS_SECRET_ACCESS_KEY:   " << aws_secret_access_key;
    LOG(INFO) << "AWS_SESSION_TOKEN:       " << aws_session_token;

    Aws::Auth::AWSCredentials credentials(Aws::String(aws_access_key_id.c_str()), Aws::String(aws_secret_access_key.c_str()), Aws::String(aws_session_token.c_str()));

    Aws::Kinesis::KinesisClient kinesisClient(credentials, clientConfig);


    // Describe shards
    Aws::Kinesis::Model::DescribeStreamRequest describeStreamRequest;
    describeStreamRequest.SetStreamName(streamName);
    Aws::Vector<Aws::Kinesis::Model::Shard> shards;
    Aws::String exclusiveStartShardId = "";
    do
    {
      Aws::Kinesis::Model::DescribeStreamOutcome describeStreamResult = kinesisClient.DescribeStream(describeStreamRequest);
      Aws::Vector<Aws::Kinesis::Model::Shard> shardsTemp = describeStreamResult.GetResult().GetStreamDescription().GetShards();
      shards.insert(shards.end(), shardsTemp.begin(), shardsTemp.end());
      std::cout << describeStreamResult.GetError().GetMessage();
      if (describeStreamResult.GetResult().GetStreamDescription().GetHasMoreShards() && shards.size() > 0)
      {
        exclusiveStartShardId = shards[shards.size() - 1].GetShardId();
        describeStreamRequest.SetExclusiveStartShardId(exclusiveStartShardId);
      }
      else
        exclusiveStartShardId = "";
    } while (exclusiveStartShardId.length() != 0);

    if (shards.size() > 0)
    {
      std::cout << "Shards found:" << std::endl;
      for (auto shard : shards)
      {
        std::cout << shard.GetShardId() << std::endl;
      }

      Aws::Kinesis::Model::GetShardIteratorRequest getShardIteratorRequest;
      getShardIteratorRequest.SetStreamName(streamName);
      // use the first shard found
      getShardIteratorRequest.SetShardId(shards[0].GetShardId());
      getShardIteratorRequest.SetShardIteratorType(Aws::Kinesis::Model::ShardIteratorType::TRIM_HORIZON);

      Aws::Kinesis::Model::GetShardIteratorOutcome getShardIteratorResult = kinesisClient.GetShardIterator(getShardIteratorRequest);
      Aws::String shardIterator = getShardIteratorResult.GetResult().GetShardIterator();

      //getRecordsRequest.SetLimit(25);
      //getRecordsRequest.S

      // pull down 100 records
      //std::cout << "Retrieving 100 records" << std::endl;
      size_t sz=0;
      while(true)
      {
        Aws::Kinesis::Model::GetRecordsRequest getRecordsRequest;
        getRecordsRequest.SetShardIterator(shardIterator);
        Aws::Kinesis::Model::GetRecordsOutcome getRecordsResult = kinesisClient.GetRecords(getRecordsRequest);
        for (auto r : getRecordsResult.GetResult().GetRecords())
        {
          Aws::String s((char*)r.GetData().GetUnderlyingData());
          std::cout << s.substr(0, r.GetData().GetLength()) << " aprox toa: " << r.GetApproximateArrivalTimestamp().CalculateGmtTimeWithMsPrecision() << std::endl;
          sz++;
        }
        LOG(INFO) << "got " << sz << ", at " << milliseconds_since_epoch();
        shardIterator = getRecordsResult.GetResult().GetNextShardIterator();
      }
    }
  }
  Aws::ShutdownAPI(options);

  return 0;
}


