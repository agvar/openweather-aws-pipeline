variable "create_ephemeral_resources_flag"{
  description = "Create ephemeral resources"
  type = bool
  default = true
}

#################################
# Persistent resources
#################################

resource "aws_s3_bucket" "weatherDataCode"{
  bucket = "weatherdatacode-bucket-11-07-2025-12-20-AM-avar"

}

resource "aws_iam_role" "lambda_execution_role"{
  name = "weather-lambda-execution-role"
  assume_role_policy =  jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })

}

#################################
#Ephemeral Resources
#################################

resource "aws_lambda_function" "weather_collector_lambda"{
  count = var.create_ephemeral_resources ? 1 : 0
  function_name="weather-collector-lambda"
  role = "aws_iam_role"."lambda_execution_role".arn
  handler = "WeatherDataCollector.lambda_handler"
  runtime = "python3.9"
  timeout = 300
  s3_bucket = "aws_s3_bucket"."weatherDataCode".bucket
  s3_key = "weather-collector.zip"

}

resource "aws_cloudwatch_event_rule" "weather_collection_schedule" {
  count = var.create_ephemeral_resources ? 1 :0 
  name                = "weather-collection-schedule"
  count = var.create_ephemeral_resources ? 1 : 0
  description         = "Trigger weather collection twice daily"
  schedule_expression = "cron(0 6,18 * * ? *)"
}

resource "aws_cloudwatch_log_group" "weather_collector_lambda_logs"{
  count = var.create_ephemeral_resources ? 1 :0 
  name = "/aws/lambda/weather-collector-lambda-logs 
  retention_in_days = 15
}

