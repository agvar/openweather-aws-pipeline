provider "aws"{
  region = "us-east-1"
}

data "aws_iam_role" "github_actions_aws"{
  name="github-actions-weather-app"
}

###########################
# Refer Existing resources
###########################

data "aws_s3_bucket" "weatherDataStore"{
  bucket = "weather-data-store-bucket-11-07-2025-12-20-avar"
}

data "aws_s3_bucket" "weatherDataCode"{
  bucket = "weather-code-bucket-11-07-2025-12-20-avar"
}

data "aws_iam_role" "lambda_execution_role"{
  name = "weather-lambda-execution-role"
}

data "aws_s3_object" "lambda_code" {
  bucket = data.aws_s3_bucket.weatherDataCode.bucket
  key    = "weather-collector.zip"
}

###################
# Create Resources
###################

resource "aws_dynamodb_table" "weather_collection_queue" {
  name = "weather_collection_queue"
  billing_mode = "PAY_PER_REQUEST"
  hash_key = "item_id"

  attribute {
    name = "item_id"
    type = "S"
  }

  attribute {
    name = "status"
    type = "S"
  }

  attribute {
    name = "date"
    type = "S"
  }

  global_secondary_index {
    name = "status-date-index"
    hash_key = "status"
    range_key = "date"
    projection_type = "ALL"
  }
}

resource "aws_dynamodb_table" "weather_collection_progress"{
  name = "weather_collection_progress"
  billing_mode = "PAY_PER_REQUEST"
  hash_key = "job_id"

  attribute {
    name = "job_id"
    type = "S"
  }
}

resource "aws_lambda_layer_version" "dependencies_layer"{
  layer_name= "weather-collector-dependencies"
  s3_bucket = data.aws_s3_bucket.weatherDataCode.bucket
  s3_key = "lambda_layer.zip"
  compatible_runtimes = ["python3.11"]
}

resource "aws_lambda_function" "weather_collector_lambda"{
  function_name="weather-collector-lambda"
  role = data.aws_iam_role.lambda_execution_role.arn
  handler = "weather_collector_lambda_handler.lambda_handler"
  runtime = "python3.11"
  timeout = 300
  s3_bucket = data.aws_s3_bucket.weatherDataCode.bucket
  s3_key = "weather-collector.zip"
  source_code_hash = data.aws_s3_object.lambda_code.etag
  layers=[aws_lambda_layer_version.dependencies_layer.arn]
}

resource "aws_lambda_function" "weather_history_gen_lambda"{
  function_name="weather-history-gen-lambda"
  role = data.aws_iam_role.lambda_execution_role.arn
  handler = "weather_hist_gen_lambda_handler.histGen_lambda_handler"
  runtime = "python3.11"
  timeout = 300
  s3_bucket = data.aws_s3_bucket.weatherDataCode.bucket
  s3_key = "weather-collector.zip"
  source_code_hash = data.aws_s3_object.lambda_code.etag
  layers=[aws_lambda_layer_version.dependencies_layer.arn]
}
resource "aws_cloudwatch_event_rule" "weather_collection_schedule" {
  name  = "weather-collection-schedule"
  description         = "Trigger weather collection twice daily"
  schedule_expression = "cron(0 6 * * ? *)"
 }

resource "aws_cloudwatch_event_target" "trigger_lambda"{
  rule = aws_cloudwatch_event_rule.weather_collection_schedule.name
  arn = aws_lambda_function.weather_collector_lambda.arn
  target_id = "weather-lambda-target"
}

resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.weather_collector_lambda.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.weather_collection_schedule.arn
}