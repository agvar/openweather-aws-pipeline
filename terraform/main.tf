provider "aws"{
  region = "us-east-1"
}

variable "create_ephemeral_resources_flag"{
  description = "Create ephemeral resources"
  type = bool
  default = true
}

data "aws_iam_role" "github_actions_aws"{
  name="github-actions-weather-app"
}

############################
# Refer Persistent resources
############################

data "aws_s3_bucket" "weatherDataStore"{
  bucket = "weather-data-store-bucket-11-07-2025-12-20-avar"
}

data "aws_s3_bucket" "weatherDataCode"{
  bucket = "weather-code-bucket-11-07-2025-12-20-avar"
}

data "aws_iam_role" "lambda_execution_role"{
  name = "weather-lambda-execution-role"
}


########################################
# Create and Destroy Ephemeral Resources
########################################

resource "aws_lambda_layer_version" "dependencies_layer"{
  layer_name= "weather-collector-dependencies"
  s3_bucket = data.aws_s3_bucket.weatherDataCode.bucket
  s3_key = "lambda_layer.zip"
  compatible_runtimes = ["python3.9"]
}

resource "aws_lambda_function" "weather_collector_lambda"{
  count = var.create_ephemeral_resources_flag ? 1 : 0
  function_name="weather-collector-lambda"
  role = data.aws_iam_role.lambda_execution_role.arn
  handler = "WeatherCollector_lambda_handler.lambda_handler"
  runtime = "python3.9"
  timeout = 300
  s3_bucket = data.aws_s3_bucket.weatherDataCode.bucket
  s3_key = "weather-collector.zip"
  layers=[aws_lambda_layer_version.dependencies_layer.arn]
}

resource "aws_cloudwatch_event_rule" "weather_collection_schedule" {
  count = var.create_ephemeral_resources_flag ? 1 :0
  name  = "weather-collection-schedule"
  description         = "Trigger weather collection twice daily"
  schedule_expression = "cron(0 6,18 * * ? *)"
 }
resource "aws_cloudwatch_event_target" "trigger_lambda"{
  count = var.create_ephemeral_resources_flag ? 1 :0
  rule = aws_cloudwatch_event_rule.weather_collection_schedule[0].name
  arn = aws_lambda_function.weather_collector_lambda[0].arn
}

resource "aws_lambda_permission" "allow_eventbridge" {
  count         = var.create_ephemeral_resources_flag ? 1 : 0
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.weather_collector_lambda[0].function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.weather_collection_schedule[0].arn
}

resource "aws_iam_role_policy" "lambda_execution_policy"{
    role = data.aws_iam_role.lambda_execution_role.id
    policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:*:*:*"
    },

      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          data.aws_s3_bucket.weatherDataStore.arn,
          "${data.aws_s3_bucket.weatherDataStore.arn}/*",
          data.aws_s3_bucket.weatherDataCode.arn,
          "${data.aws_s3_bucket.weatherDataCode.arn}/*"
        ]
      },
      {
        "Effect": "Allow",
        "Action": [
          "ssm:GetParameter"
        ],
        "Resource": "arn:aws:ssm:us-east-1:049506468119:parameter/Openweatherapi_key"
      }
    ]
    })
    }